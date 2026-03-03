import os
import time
import csv
from datetime import datetime
import serial

# ============================================================
# CONFIGURATION – CHANGE THESE VALUES FOR YOUR SETUP
# ============================================================

# Serial port where the XBee is connected
COM_PORT = ["COM8", "COM7"]

# Serial communication speed
BAUDRATE = 9600          # adjust to your XBee / sensor settings

# Sensor IDs of the ultrasonic anemometers
# Example: ["01", "02", "03"]
SENSOR_IDS = ["01", "02"]

# Telegram type (your example: "05", David also mentioned 1–12)
TELEGRAM_TYPE = "08"

# Prefix used in the command, e.g. 'tr' or 'TR'
TELEGRAM_PREFIX = "TR"

# Number of telegrams per second **per sensor**
TELEGRAMS_PER_SECOND = 1.0   # e.g. 1.0 = 1x/s, 2.0 = 2x/s, ...

# Folder where log files will be stored
LOG_DIR = "logs"

# Serial read timeout in seconds
READ_TIMEOUT = 0.5


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def ensure_log_dir(path: str) -> None:
    """Create log directory if it does not exist."""
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def open_serial(port: str, baud: int, timeout: float) -> serial.Serial:
    """Open and return a configured serial.Serial object."""
    ser = serial.Serial(port=port, baudrate=baud, timeout=timeout)
    # Optional: clear input buffer
    ser.reset_input_buffer()
    ser.reset_output_buffer()
    return ser


def build_command(sensor_id: str, prefix: str, t_type: str) -> bytes:
    """
    Build the command string for a given sensor.
    Example: sensor '02', prefix 'tr', type '05' -> '02tr05\r\n'
    """
    cmd_str = f"{sensor_id}{prefix}{t_type}\r\n"
    return cmd_str.encode("ascii")


def open_new_log_file(base_dir: str) -> tuple[csv.writer, any, datetime]:
    """
    Open a new CSV log file for the current hour.
    Returns (csv_writer, file_handle, hour_start).
    """
    now = datetime.now()
    # Hour "bucket" start (minute=0,second=0)
    hour_start = now.replace(minute=0, second=0, microsecond=0)

    filename = f"usa_log_{hour_start.strftime('%Y-%m-%d_%H')}.csv"
    full_path = os.path.join(base_dir, filename)

    f = open(full_path, mode="a", newline="", encoding="utf-8")
    writer = csv.writer(f, delimiter=';')

    # If file is new/empty, write header
    if os.path.getsize(full_path) == 0:
        writer.writerow(["timestamp", "com_port", "sensor_id",
                         "telegram_type", "data_raw"])

    print(f"[INFO] Logging to {full_path}")
    return writer, f, hour_start


# ============================================================
# MAIN LOGGING LOOP
# ============================================================

def main():
    ensure_log_dir(LOG_DIR)

    # Convert to lists if they are single strings
    ports = COM_PORT if isinstance(COM_PORT, list) else [COM_PORT]
    sensor_ids = SENSOR_IDS if isinstance(SENSOR_IDS, list) else [SENSOR_IDS]

    # 1. Open distinct serial ports
    active_ports = {}
    for p in set(ports):  # Unique ports
        print(f"[INFO] Opening serial port {p} at {BAUDRATE} baud...")
        try:
            ser = open_serial(p, BAUDRATE, READ_TIMEOUT)
            active_ports[p] = ser
        except Exception as e:
            print(f"[ERROR] Could not open serial port {p}: {e}")

    if not active_ports:
        print("[ERROR] No valid serial ports could be opened. Exiting.")
        return

    # 2. Map commands to execute: (port_name, serial_obj, sensor_id)
    # If only 1 port is provided but multiple sensors, all sensors use that port
    # Otherwise, we pair them up by index, reusing the last port if sensors > ports
    tasks = []
    if len(ports) == 1:
        p = ports[0]
        if p in active_ports:
            for sid in sensor_ids:
                tasks.append((p, active_ports[p], sid))
    else:
        for i, sid in enumerate(sensor_ids):
            p = ports[i] if i < len(ports) else ports[-1]
            if p in active_ports:
                tasks.append((p, active_ports[p], sid))

    # Prepare logging
    writer, log_file, current_hour_start = open_new_log_file(LOG_DIR)

    if TELEGRAMS_PER_SECOND <= 0:
        loop_interval = 1.0
    else:
        loop_interval = 1.0 / TELEGRAMS_PER_SECOND

    print(f"[INFO] Starting logging loop. Target interval: {loop_interval:.2f}s per cycle. Press Ctrl+C to stop.")

    try:
        while True:
            loop_start = time.time()
            
            # Check if we need a new file (new hour)
            now = datetime.now()
            hour_bucket = now.replace(minute=0, second=0, microsecond=0)
            if hour_bucket > current_hour_start:
                # Close old file and open a new file
                print("[INFO] New hour detected, rotating log file...")
                log_file.close()
                writer, log_file, current_hour_start = open_new_log_file(LOG_DIR)

            for port_name, ser, sensor_id in tasks:
                # 1) Build and send the command
                cmd = build_command(sensor_id, TELEGRAM_PREFIX, TELEGRAM_TYPE)
                try:
                    ser.write(cmd)
                    # ser.flush() # Removed flush to prevent issues on some platforms when swapping multiple COM ports fast
                    print(f"[DEBUG] Sent command to {port_name}: {cmd.decode('ascii').strip()}")
                except Exception as e:
                    print(f"[ERROR] Failed to send command to sensor {sensor_id} on {port_name}: {e}")
                    continue

                # Give the sensor slightly more time to reply before trying to read
                time.sleep(0.001)

                # 2) Read response line from sensor via XBee
                try:
                    line_bytes = ser.read_until(b'\r')
                    # If there is a trailing \n, read it too so it doesn't stay in the buffer
                    if ser.in_waiting > 0:
                        peek = ser.read(ser.in_waiting)
                        if b'\n' in peek:
                            line_bytes += peek
                except Exception as e:
                    print(f"[ERROR] Failed to read from {port_name}: {e}")
                    continue

                if not line_bytes:
                    print(f"[WARN] No data from sensor {sensor_id} on {port_name} (timeout)")
                else:
                    try:
                        data_str = line_bytes.decode("ascii", errors="replace").strip()
                    except Exception:
                        data_str = repr(line_bytes)

                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                    # 3) Write to log file
                    writer.writerow([timestamp, port_name, sensor_id,
                                     TELEGRAM_TYPE, data_str])
                    log_file.flush()

                    print(f"{timestamp} | {port_name} | ID={sensor_id} | "
                          f"TYPE={TELEGRAM_TYPE} | {data_str}")

            # 4) Wait for the remainder of the loop interval to achieve the target frequency
            elapsed = time.time() - loop_start
            sleep_time = loop_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n[INFO] Stopping logger (Ctrl+C pressed).")
    finally:
        try:
            log_file.close()
        except Exception:
            pass
        for p, ser in active_ports.items():
            try:
                ser.close()
                print(f"[INFO] Closed serial port {p}.")
            except Exception:
                pass
        print("[INFO] Done.")


if __name__ == "__main__":
    main()
