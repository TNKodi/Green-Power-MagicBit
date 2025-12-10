import time
import random
import json
import paho.mqtt.client as mqtt

# ============================================
# 1. CONFIG – CHANGE THESE
# ============================================

MQTT_HOST = "cloud.thingsnode.cc"   # e.g. "localhost" or "tb.example.com"
MQTT_PORT = 1883                      # 8883 if using TLS

# Device name -> access token mapping
DEVICES = {
    "CKS1005":    "70j1UQRcYGa6SFwfteXY",
    "CML1003":    "R4IeRCWlwhNo0q4HSqBk",
    "CNH1006":    "c93hObXnRiIKuLcBBrh3",
    # "SPRSGL1002": "TOKEN_FOR_SPRSGL1002",
    # "SPRSGS1001": "TOKEN_FOR_SPRSGS1001",
    # "UBS1001":    "TOKEN_FOR_UBS1001",
    # "UMH1002":    "TOKEN_FOR_UMH1002",
    # "WPCWCH1002": "TOKEN_FOR_WPCWCH1002",
    # "WPCWGS1001": "TOKEN_FOR_WPCWGS1001",
}

# How often to send to each device
SEND_INTERVAL_SEC = 30


# ============================================
# 2. RANDOM PAYLOAD (MATCH YOUR WIDGET KEYS)
# ============================================

def generate_payload(device_name: str) -> dict:
    """
    Create random but reasonable values for a solar plant.
    Make sure keys match the widget keys in your dashboard.
    """

    power_kw = round(random.uniform(0, 200), 2)        # active power
    grid_frequency_hz = round(random.uniform(49.5, 50.5), 2)
    pr = round(random.uniform(0.7, 1.0), 3)            # performance ratio
    rp_kvar = round(random.uniform(-50, 50), 2)        # reactive power
    energy_today_kwh = round(random.uniform(0, 1000), 1)

    return {
        "power_kw": power_kw,
        "grid_frequency_hz": grid_frequency_hz,
        "pr": pr,
        "rp_kvar": rp_kvar,
        "energy_today_kwh": energy_today_kwh,
        # add/remove keys to fit your dashboard
    }


# ============================================
# 3. SEND ONE MQTT MESSAGE FOR ONE DEVICE
# ============================================

def send_telemetry_mqtt(device_name: str, token: str):
    """
    Connects as that device (token = username),
    publishes one telemetry JSON, then disconnects.
    """
    payload = generate_payload(device_name)
    payload_str = json.dumps(payload)

    client = mqtt.Client(client_id=f"sim_{device_name}")
    client.username_pw_set(token)  # token as username, no password

    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()

        # Telemetry topic for ThingsBoard
        result = client.publish("v1/devices/me/telemetry", payload_str, qos=1)
        result.wait_for_publish()

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"[OK] {device_name} -> {payload}")
        else:
            print(f"[ERROR] publish for {device_name}, rc={result.rc}")

        client.loop_stop()
        client.disconnect()

    except Exception as e:
        print(f"[EXCEPTION] {device_name}: {e}")


# ============================================
# 4. MAIN LOOP
# ============================================

def main():
    print("Starting MQTT solar plant simulator...")
    print(f"MQTT broker: {MQTT_HOST}:{MQTT_PORT}")
    print(f"{len(DEVICES)} devices, interval {SEND_INTERVAL_SEC}s\n")
    print("Press Ctrl+C to stop.\n")

    while True:
        for device_name, token in DEVICES.items():
            send_telemetry_mqtt(device_name, token)
        time.sleep(SEND_INTERVAL_SEC)


if __name__ == "__main__":
    main()
