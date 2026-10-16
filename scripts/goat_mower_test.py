#!/usr/bin/env python3
"""Manual test for starting an Ecovacs GOAT mower.

Run it locally against a real mower to check which start command the mower
answers. Credentials are read from environment variables only:

    ECOVACS_ACCOUNT    account e-mail (required)
    ECOVACS_PASSWORD   account password (required)
    ECOVACS_COUNTRY    alpha-2 country code, default "DE"
    ECOVACS_DEVICE_ID  optional client id; default is derived from the account,
                       so the e-mail device verification is needed only once

The script logs in, lists the devices, reads the state and only sends the
command after you type "ja". The request and the raw answer are printed.
Afterwards it listens on MQTT for a while and prints what the mower pushes.

Example:
    uv run python scripts/goat_mower_test.py --variant mower --action start

"""

# ruff: noqa: T201

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import TYPE_CHECKING, Any

import aiohttp
import orjson

from deebot_client.api_client import ApiClient
from deebot_client.authentication import Authenticator, create_rest_config
from deebot_client.capabilities import DeviceType
from deebot_client.commands.json.battery import GetBattery
from deebot_client.commands.json.charge_state import GetChargeState
from deebot_client.commands.json.clean import (
    Clean,
    CleanV2,
    GetCleanInfo,
    GetCleanInfoV2,
)
from deebot_client.commands.json.common import ExecuteCommand
from deebot_client.event_bus import EventBus
from deebot_client.exceptions import ApiTimeoutError, DeviceVerificationRequiredError
from deebot_client.models import CleanAction
from deebot_client.mqtt_client import MqttClient, SubscriberInfo, create_mqtt_config
from deebot_client.util import md5

try:
    from deebot_client.commands.json.clean import CleanMower
except ImportError:  # released library without the fix
    CleanMower = None  # type: ignore[assignment,misc]

if TYPE_CHECKING:
    from deebot_client.command import Command
    from deebot_client.models import ApiDeviceInfo, DeviceInfo


# Candidate start commands. "mower" is what the fixed library sends.
VARIANTS: dict[str, str] = {
    "mower": 'clean     {"act": <action>, "content": {"type": "auto"}}  (fix, app capture)',
    "v2": 'clean_V2  {"act": <action>, "content": {"type": "auto"}}  (old behaviour)',
    "flat": 'clean     {"act": <action>, "type": "auto"}',
    "verb": 'clean     {"act": <action>}',
    "custom": "--name and --payload as given",
}


def _json(data: Any) -> str:
    return orjson.dumps(data, option=orjson.OPT_INDENT_2, default=str).decode()


def _raw_command(name: str, args: dict[str, Any]) -> ExecuteCommand:
    """Create a command with an arbitrary name and body.data."""
    command_type: type[ExecuteCommand] = type(
        "RawCommand", (ExecuteCommand,), {"NAME": name}
    )
    return command_type(args)


def build_command(args: argparse.Namespace) -> Command:
    """Build the command for the selected variant."""
    action = CleanAction(args.action)
    command: Command
    match args.variant:
        case "mower":
            if CleanMower is not None:
                command = CleanMower(action)
            else:
                command = _raw_command(
                    Clean.NAME, {"act": action.value, "content": {"type": "auto"}}
                )
        case "v2":
            command = CleanV2(action)
            if action is not CleanAction.START:
                # CleanV2 sends an empty type here, which mowers reject.
                command = _raw_command(
                    CleanV2.NAME, {"act": action.value, "content": {"type": "auto"}}
                )
        case "flat":
            command = _raw_command(Clean.NAME, {"act": action.value, "type": "auto"})
        case "verb":
            command = _raw_command(Clean.NAME, {"act": action.value})
        case "custom":
            if not args.name or not args.payload:
                sys.exit("--variant custom needs --name and --payload")
            command = _raw_command(args.name, orjson.loads(args.payload))
        case _:
            sys.exit(f"Unknown variant {args.variant}")

    if args.ver:
        _override_header_version(command, args.ver)
    return command


def _override_header_version(command: Command, version: str) -> None:
    """Replace header.ver of the payload (library default is "0.0.50")."""
    get_payload = command._get_payload  # noqa: SLF001

    def _get_payload() -> Any:
        payload = get_payload()
        if isinstance(payload, dict) and "header" in payload:
            payload["header"]["ver"] = version
        return payload

    command._get_payload = _get_payload  # type: ignore[method-assign] # noqa: SLF001


async def send_raw(
    authenticator: Authenticator, api_info: ApiDeviceInfo, command: Command
) -> None:
    """Send the command as it is and print request and raw response.

    The REST call is used directly, so the library's start/resume rewrite and
    response handling do not change what is sent or hide the answer.
    """
    print(f"\n>>> {command.NAME}")
    print(_json(command._get_payload()))  # noqa: SLF001
    try:
        response = await command._execute_api_request(authenticator, api_info)  # noqa: SLF001
    except ApiTimeoutError as ex:
        print(f"<<< Timeout: {ex}")
        return
    print("<<< raw response")
    print(_json(response))


async def login(
    authenticator: Authenticator, api_client: ApiClient
) -> list[DeviceInfo]:
    """Log in (with e-mail verification if needed) and return MQTT devices."""
    try:
        devices = await api_client.get_devices()
    except DeviceVerificationRequiredError:
        print("Ecovacs requires a one-time e-mail verification for this client id.")
        await authenticator.request_device_verification_code()
        code = await asyncio.to_thread(input, "Code from the e-mail: ")
        await authenticator.verify_device(code)
        devices = await api_client.get_devices()

    print("\nDevices:")
    for info in devices.mqtt:
        api = info.api
        print(
            f"  [mqtt]        {api.get('deviceName', '')} "
            f"nick={api.get('nick', '')!r} class={api['class']} did={api['did']} "
            f"type={info.static.capabilities.device_type}"
        )
    for api in devices.not_supported:
        print(f"  [unsupported] {api.get('deviceName', '')} class={api['class']}")
    for api in devices.xmpp:
        print(f"  [xmpp]        {api.get('deviceName', '')} class={api['class']}")
    return devices.mqtt


def select_device(devices: list[DeviceInfo], did: str | None) -> DeviceInfo:
    """Select the mower to test."""
    if did:
        for info in devices:
            if info.api["did"] == did:
                return info
        sys.exit(f"No device with did {did}")
    mowers = [
        info
        for info in devices
        if info.static.capabilities.device_type is DeviceType.MOWER
    ]
    if len(mowers) != 1:
        sys.exit(f"Found {len(mowers)} mowers, please choose one with --did")
    return mowers[0]


async def main(args: argparse.Namespace) -> None:
    """Run the test."""
    try:
        account = os.environ["ECOVACS_ACCOUNT"]
        password = os.environ["ECOVACS_PASSWORD"]
    except KeyError as ex:
        sys.exit(f"Environment variable {ex} is missing")
    country = os.environ.get("ECOVACS_COUNTRY", "DE").upper()
    device_id = os.environ.get("ECOVACS_DEVICE_ID") or md5(f"goat-test-{account}")

    command = build_command(args)

    async with aiohttp.ClientSession() as session:
        rest_config = create_rest_config(
            session, device_id=device_id, alpha_2_country=country
        )
        authenticator = Authenticator(rest_config, account, md5(password))
        devices = await login(authenticator, ApiClient(authenticator))
        info = select_device(devices, args.did)
        api_info = info.api
        print(f"\nUsing {api_info.get('deviceName', '')} class={api_info['class']}")
        print(
            "Library command for this class: "
            f"{info.static.capabilities.clean.action.command.__name__}"
        )

        mqtt = MqttClient(
            create_mqtt_config(device_id=device_id, country=country), authenticator
        )

        async def no_commands(_: Command) -> None:
            """Only the commands printed by this script are sent."""

        # Messages are only printed, not handled, so nothing is sent in between.
        events = EventBus(no_commands, info.static.capabilities)  # type: ignore[arg-type]

        def on_message(name: str, data: str | bytes | bytearray) -> None:
            text = data.decode() if isinstance(data, bytes | bytearray) else data
            print(f"[mqtt] {name}: {text}")

        unsubscribe = await mqtt.subscribe(SubscriberInfo(info, events, on_message))

        try:
            print("\n--- state (getCleanInfo_V2 may time out after ~20 s) ---")
            for get in (
                GetBattery(),
                GetChargeState(),
                GetCleanInfo(),
                GetCleanInfoV2(),
            ):
                await send_raw(authenticator, api_info, get)

            print("\n--- command to send ---")
            print(f"variant {args.variant}: {VARIANTS[args.variant]}")
            print(f">>> {command.NAME}")
            print(_json(command._get_payload()))  # noqa: SLF001
            answer = await asyncio.to_thread(
                input, '\nSend this command to the mower? Type "ja" to send: '
            )
            if answer.strip().lower() != "ja":
                print("Not sent.")
                return

            await send_raw(authenticator, api_info, command)

            print(f"\n--- listening {args.listen} s for MQTT messages ---")
            await asyncio.sleep(args.listen)
        finally:
            unsubscribe()
            await mqtt.disconnect()


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--variant",
        choices=VARIANTS,
        default="mower",
        help="; ".join(f"{k}: {v}" for k, v in VARIANTS.items()),
    )
    parser.add_argument(
        "--action", choices=[a.value for a in CleanAction], default="start"
    )
    parser.add_argument("--ver", help='override header.ver, e.g. "0.0.22"')
    parser.add_argument("--name", help="command name for --variant custom")
    parser.add_argument("--payload", help="body.data JSON for --variant custom")
    parser.add_argument("--did", help="device id, if there is more than one mower")
    parser.add_argument(
        "--listen", type=int, default=60, help="seconds to print MQTT messages"
    )
    parser.add_argument("--debug", action="store_true", help="library debug logs")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    logging.basicConfig(level=logging.DEBUG if arguments.debug else logging.WARNING)
    asyncio.run(main(arguments))
