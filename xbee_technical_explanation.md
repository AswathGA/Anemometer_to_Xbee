# Technical Explanation of `xbee.py`

The `xbee.py` script is a polling-based data logging application designed to communicate with ultrasonic anemometers (wind sensors) over a serial connection, presumably utilizing XBee wireless modules as transparent serial bridges. It continuously requests data telegrams from one or more sensors and records their responses into hourly-rotated CSV log files.

## Core Architecture and Lifecycle

1. **Initialization**:
   - The script begins by ensuring the target log directory (`logs`) exists using `os.makedirs`.
   - It establishes a serial connection using the `pyserial` library (`serial.Serial`) to the specified `COM_PORT` (e.g., `COM7`) at a set `BAUDRATE` (e.g., `9600`).
   - Input and output buffers are reset upon connection to drop any stale bytes.

2. **Hourly Log Rotation Mechanism**:
   - Log files are managed by the `open_new_log_file` function. They follow the naming convention `usa_log_YYYY-MM-DD_HH.csv`.
   - The script tracks the current `hour_start` bucket (e.g., 14:00:00). During its infinite polling loop, it checks if the current time exceeds this bucket.
   - When the hour changes, it gracefully closes the active file handle and opens a new one, writing the CSV header if it's a completely new file.

3. **Sensor Polling Loop**:
   - Inside an infinite `while True:` loop, the script iterates through a configured list of `SENSOR_IDS`.
   - **Command Construction**: For each sensor, it builds an ASCII command string formatted as `{sensor_id}{prefix}{telegram_type}\r\n` (e.g., `02TR08\r\n`).
   - **Transmission**: The command is encoded to bytes and sent out over the serial port via `ser.write()`. `ser.flush()` ensures the command is physically sent out of the serial buffer.

4. **Data Reception and Handling**:
   - The script uses `ser.readline()` to wait for a response, terminating when a newline character is encountered or when the `READ_TIMEOUT` (0.5s) expires.
   - If data is received, it decodes the ASCII bytes (`errors="replace"` ensures faulty bytes don't crash the script) and strips whitespace/newlines.
   - It generates a high-precision timestamp (down to milliseconds).

5. **Persistent Storage (Disk I/O)**:
   - The received data string is written as a delimited row to the active CSV file alongside the timestamp, COM port, sensor ID, and telegram type.
   - **Crucial step**: `log_file.flush()` is called immediately after writing. This pushes the data out of the application-level buffer directly to the OS disk buffer, minimizing data loss if the system abruptly loses power or crashes.

6. **Rate Limiting (Throttling)**:
   - To avoid flooding the serial network or the sensors, an artificial delay is calculated based on `TELEGRAMS_PER_SECOND` (e.g., `1.0 / TELEGRAMS_PER_SECOND`). The loop sleeps for this duration before addressing the next sensor.

7. **Graceful Degradation and Shutdown**:
   - **Error Catching**: Serial read/write operations are wrapped in `try...except` blocks. If communication with a single sensor fails, the script logs the error and `continue`s to the next iteration rather than crashing completely.
   - **Shutdown**: A `try...finally` block wraps the entire loop. Intercepting a `KeyboardInterrupt` (Ctrl+C) ensures the script safely closes the CSV file handle and the serial port before exiting, preventing port lockups and file corruption.

## Key Configuration Variables
The script is heavily parameterized at the top, making it easily adaptable:
*   `COM_PORT` & `BAUDRATE`: Standard serial connection settings.
*   `SENSOR_IDS`: Array of string IDs for addressable sensors on the shared network.
*   `TELEGRAM_PREFIX` & `TELEGRAM_TYPE`: Proprietary protocol headers specific to these anemometers.
*   `TELEGRAMS_PER_SECOND`: Dictates the polling frequency per sensor.
*   `READ_TIMEOUT`: Prevents the script from hanging indefinitely if a sensor drops offline.
