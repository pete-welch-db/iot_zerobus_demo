import argparse
import base64
import hashlib
import hmac
import time
import urllib.parse


def generate_sas_token(resource_uri: str, key: str, expiry_epoch: int) -> str:
    encoded_uri = urllib.parse.quote_plus(resource_uri)
    sign_data = f"{encoded_uri}\n{expiry_epoch}".encode("utf-8")
    key_bytes = base64.b64decode(key)
    signature = hmac.new(key_bytes, sign_data, hashlib.sha256).digest()
    encoded_sig = urllib.parse.quote_plus(base64.b64encode(signature))
    return f"SharedAccessSignature sr={encoded_uri}&sig={encoded_sig}&se={expiry_epoch}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate an Azure IoT Hub device SAS token.")
    parser.add_argument("--resource-uri", required=True, help="IoT Hub device URI, e.g. myhub.azure-devices.net/devices/iotdev-0000")
    parser.add_argument("--device-key", required=True, help="Base64 encoded device primary/secondary key")
    parser.add_argument("--ttl-seconds", type=int, default=3600, help="Token lifetime in seconds")
    args = parser.parse_args()

    expiry = int(time.time()) + args.ttl_seconds
    token = generate_sas_token(args.resource_uri, args.device_key, expiry)
    print(token)


if __name__ == "__main__":
    main()
