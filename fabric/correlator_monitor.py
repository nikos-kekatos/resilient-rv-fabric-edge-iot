#!/usr/bin/env python3
"""
Layer 3 Correlator - MonPoly Online MFOTL Monitor + TeSSLA
Online/incremental event-based temporal logic monitoring

Properties:
- P3.1: Multi-Vector APT Detection (60s window)
- P3.2: Coordinated Attack Detection (30s window)
- P3.3: Attack Escalation Pattern (10-60s window)
- P3.4: Persistent Campaign Detection (1h window)
- P3.5: RTLola Silent Node Anomaly (5 minutes)
"""

import sys
import json
import subprocess
import threading
from collections import defaultdict, deque

import subprocess
import threading
import time
import re
from collections import deque


class OnlineRTLolaMonitor:
    """
    Layer 3 Monitor using RTLola (Stream Runtime Verification)
    Single process - native parameterization handles multiple nodes.
    Time-triggered: violations fire automatically at deadline without new events.
    """
    
    # Regex for RTLola output:
    # [5.000022416][Trigger][#0(node-1)][Value] = "silent_node_anomaly: node-1"
    TRIGGER_RE = re.compile(
        r'\[(?P<time>[\d.]+)\]\[Trigger\]\[#\d+\((?P<device>[^)]+)\)\]\[Value\]\s*=\s*"(?P<msg>[^"]+)"'
    )

    def __init__(self, rtlola_file="silent_node.lola"):
        print("=== Started online monitor for P3.5 (RTLola Time-Triggered) ===", flush=True)
        
        self.rtlola_dir = "/app/rtlola_specs"
        self.rtlola_file = f"{self.rtlola_dir}/{rtlola_file}"
        self.incidents_emitted = set()
        
        self.process = None
        self.violations_queue = deque()
        self.pending_incidents = deque() 
        self.lock = threading.Lock()
        self.start_time = None  # wall-clock reference for offset timestamps
        self.running = True 

        self.last_overflow_time = {}        # device -> event timestamp (from upstream)
        self.wall_clock_at_overflow = {}    # device -> wall-clock when we call RTLola
        self._spawn_monitor()

        threading.Thread(target=self._dispatch_loop, daemon=True).start()

    def _spawn_monitor(self):
        """Initiates a unique RTLola process in online mode."""
        try:
            self.process = subprocess.Popen(
                ['stdbuf', '-oL', '-eL', 'rtlola-cli', 'monitor', '--online', '--stdin', self.rtlola_file],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            # Send CSV header first (is required from rtlola-cli)
            self.process.stdin.write("overflow,safe_tx\n")
            self.process.stdin.flush()
            
            self.start_time = time.time()
            
            # Threads for reading output
            threading.Thread(target=self._read_violations, daemon=True).start()
            threading.Thread(target=self._read_errors, daemon=True).start()
            
            print("[*] Spawned RTLola monitor (single process, parameterized)", flush=True)
        except Exception as e:
            print(f"❌ Failed to start RTLola Engine: {e}", flush=True)
            self.process = None

    def _read_violations(self):
        try:
            for line in self.process.stdout:
                line = line.strip()
                # print(f"[RTLola stdout] {line}", flush=True)   # ⭐ DEBUG
                match = self.TRIGGER_RE.search(line)
                if match:
                    with self.lock:
                        self.violations_queue.append({
                            'rtlola_time': float(match.group('time')),
                            'wall_timestamp': time.time(), 
                            'device': match.group('device'),
                            'msg': match.group('msg')
                        })
        except Exception as e:
            print(f"⚠️ Error reading RTLola output: {e}", flush=True)

    def _dispatch_loop(self):
        while self.running:
            time.sleep(0.2)
            incidents = self._drain_violations()
            with self.lock:
                for inc in incidents:
                    self.pending_incidents.append(inc)

    def _read_errors(self):
        if not self.process:
            return
        for line in self.process.stderr:
            line = line.strip()
            if line:
                print(f"⚠️ [RTLola] {line}", flush=True)

    def _drain_violations(self):
        """Επιστρέφει όλα τα pending violations από το queue."""
        results = []
        with self.lock:
            while self.violations_queue:
                violation = self.violations_queue.popleft()
                incident = self._build_incident(violation)
                if incident:
                    results.append(incident)
        return results
    
    def get_pending_incidents(self):
        with self.lock:
            results = list(self.pending_incidents)
            self.pending_incidents.clear()
        return results
    
    def _build_incident(self, violation):
        violating_device = violation['device']
    
        # Το πραγματικό timestamp του violation:
        # = timestamp του overflow event που το προκάλεσε + 5s (το deadline)
        last_overflow = self.last_overflow_time.get(violating_device)
        if last_overflow is not None:
            violation_timestamp = last_overflow + 5.0
        else:
            # Fallback: wall-clock όταν το λάβαμε (λιγότερο ακριβές)
            violation_timestamp = violation['wall_timestamp']
    
        # Deduplication: ένα incident ανά (device, overflow event)
        if last_overflow is None:
            incident_key = f"rtlola_{violating_device}_fallback_{violation['wall_timestamp']}"
        else:
            incident_key = f"rtlola_{violating_device}_{last_overflow}"
    
        if incident_key in self.incidents_emitted:
            return None
        self.incidents_emitted.add(incident_key)
    
        return {
            'type': 'silent_node_anomaly',
            'severity': 'CRITICAL',
            'device': violating_device,
            'timestamp': violation_timestamp,
            'property': 'P3.5',
            'method': 'rtlola_time_triggered',
            'metadata': {
                'msg': violation['msg'],
                'overflow_event_timestamp': last_overflow,
                'rtlola_detection_latency_ms': (
                    (violation['wall_timestamp'] - self.wall_clock_at_overflow.get(violating_device, violation['wall_timestamp'])) * 1000
                    if violating_device in self.wall_clock_at_overflow else None
                )
            }
    }

    def process_alert(self, alert):
        device = alert['device']
        alert_type = alert['type']
        event_timestamp = float(alert['timestamp'])

        if alert_type in ["overflow", "safe_tx"]:
            if self.process and self.process.poll() is None:
                if alert_type == "overflow":
                    csv_line = f"{device},#\n"
                else:
                    csv_line = f"#,{device}\n"
        
                try:
                    self.process.stdin.write(csv_line)
                    self.process.stdin.flush()
            
                    with self.lock:
                        if alert_type == "overflow":
                            self.last_overflow_time[device] = event_timestamp
                            self.wall_clock_at_overflow[device] = time.time()  # ⭐ FIX
                    
                except BrokenPipeError:
                    print("⚠️ RTLola process pipe broken", flush=True)
                except Exception as e:
                    print(f"⚠️ Error feeding RTLola: {e}", flush=True)

        with self.lock:
            results = list(self.pending_incidents)
            self.pending_incidents.clear()
        return results

    def shutdown(self):
        self.running = False
        if self.process:
            try:
                if self.process.stdin:
                    self.process.stdin.close()
                self.process.terminate()
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            except Exception:
                pass

class OnlineTeSSLaMonitor:
    """
    Layer 3 Monitor using TeSSLa (Stream Runtime Monitoring)
    Uses 'Monitor Slicing': Dynamically spawns a TeSSLa process per node.
    """
    def __init__(self, tessla_file="p3_5_silent_node.tessla"):
        print("=== Started online monitor for P3.5 (TeSSLa Monitor Slicing) ===", flush=True)
        
        self.tessla_dir = "/app/tessla_specs"
        self.tessla_file = f"{self.tessla_dir}/{tessla_file}"
        self.incidents_emitted = set()
        
        # Λεξικό με τα ενεργά TeSSLa processes: { "node-12-mixed": subprocess.Popen }
        self.monitors = {}
        
        self.violations_queue = deque()
        self.lock = threading.Lock()

    def _spawn_monitor(self, device):
        """Ξεκινάει ένα νέο TeSSLa process αποκλειστικά για τον συγκεκριμένο κόμβο"""
        try:
            process = subprocess.Popen(
                ['tessla', 'interpreter', self.tessla_file],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            self.monitors[device] = process
            
            # Ξεκινάμε thread που θα διαβάζει τα alerts ΑΥΤΟΥ του process
            threading.Thread(target=self._read_violations, args=(device, process), daemon=True).start()
            # Προαιρετικά thread για τα errors
            threading.Thread(target=self._read_errors, args=(device, process), daemon=True).start()
            
            print(f"[*] Spawning new TeSSLa monitor for {device}", flush=True)
            return process
        except Exception as e:
            print(f"❌ Failed to start TeSSLa Engine for {device}: {e}", flush=True)
            return None

    def _read_violations(self, device, process):
        """Διαβάζει real-time τα outputs του TeSSLa για τον συγκεκριμένο κόμβο"""
        try:
            for line in process.stdout:
                line = line.strip()
                if "silent_node_alert" in line:
                    with self.lock:
                        self.violations_queue.append((device, line))
        except Exception:
            pass

    def _read_errors(self, device, process):
        for line in process.stderr:
            print(f"⚠️ [TeSSLa Error - {device}] {line.strip()}", flush=True)

    def process_alert(self, alert):
        device = alert['device']
        # Μετατροπή UNIX timestamp (δευτερόλεπτα) σε milliseconds
        timestamp_ms = int(float(alert['timestamp']) * 1000)
        alert_type = alert['type']

        # 1. Προώθηση Χρόνου (Time Broadcasting)
        # Πρέπει ΟΛΑ τα ενεργά monitors να μάθουν το νέο timestamp για να δουλέψει το delay()
        for dev, proc in self.monitors.items():
            if proc.poll() is None and proc.stdin:
                try:
                    # Το '<timestamp>:' χωρίς event προωθεί το ρολόι του TeSSLa!
                    proc.stdin.write(f"{timestamp_ms}:\n")
                    proc.stdin.flush()
                except BrokenPipeError:
                    pass

        # 2. Αποστολή του event στον σωστό κόμβο
        if alert_type in ["overflow", "safe_tx"]:
            # Αν δεν υπάρχει monitor για αυτόν τον κόμβο, το φτιάχνουμε τώρα (Lazy Instantiation)
            if device not in self.monitors:
                self._spawn_monitor(device)
            
            proc = self.monitors.get(device)
            if proc and proc.poll() is None:
                # Στέλνουμε κενό γεγονός τύπου Unit, π.χ. '1779119196000: overflow = ()'
                tessla_event = f"{timestamp_ms}: {alert_type} = ()\n"
                try:
                    proc.stdin.write(tessla_event)
                    proc.stdin.flush()
                except Exception as e:
                    print(f"⚠️ Error feeding TeSSLa stream for {device}: {e}", flush=True)

        # 3. Έλεγχος για τυχόν timeouts που έσκασαν
        with self.lock:
            if self.violations_queue:
                violating_device, violation_line = self.violations_queue.popleft()
                
                # Γραμμή: "1779119201000: silent_node_alert = ()"
                parts = violation_line.split(":")
                violation_time_ms = int(parts[0].strip())
                
                # Deduplication key (ώστε να μην βγάζει spam για το ίδιο παράθυρο)
                incident_key = f"tessla_{violating_device}_{violation_time_ms // 10000}"
                
                if incident_key not in self.incidents_emitted:
                    self.incidents_emitted.add(incident_key)
                    
                    return {
                        'type': 'silent_node_anomaly',
                        'severity': 'CRITICAL',
                        'device': violating_device,
                        'timestamp': violation_time_ms / 1000.0,
                        'property': 'no_property',
                        'method': 'tessla_monitor_slicing',
                        'metadata': {
                            'msg': "Watchdog Timeout: Node failed to send safe_tx within 5s after an overflow."
                        }
                    }
        return None

    def shutdown(self):
        for dev, proc in self.monitors.items():
            try:
                if proc.stdin:
                    proc.stdin.close()
                proc.terminate()
            except Exception:
                pass

class OnlineMonPolyMonitor:
    """
    Layer 3 Monitor using MonPoly in online/incremental mode
    Persistent MonPoly processes fed incrementally via stdin
    """

    def __init__(self):
        print("=== Layer 3 MonPoly Monitor (Online MFOTL + RTLola) ===", flush=True)
        print("    Incremental event-based monitoring\n", flush=True)

        # MonPoly setup
        self.monpoly_dir = "/app/monpoly_specs"
        self.signature_file = f"{self.monpoly_dir}/signature.sig"

        # Property files
        self.properties = {
            'P3.1': f"{self.monpoly_dir}/p3_1_apt.mfotl",
            'P3.2': f"{self.monpoly_dir}/p3_2_botnet.mfotl",
            'P3.3': f"{self.monpoly_dir}/p3_3_escalation.mfotl",
            'P3.4': f"{self.monpoly_dir}/p3_4_persistent.mfotl"
        }

        # Persistent MonPoly processes (one per property)
        self.monitors = {}
        self.monitor_threads = {}
        self.monitor_status = {}  # Track which monitors are alive

        # Violation queues (from monitor threads)
        self.violation_queues = {
            'P3.1': deque(),
            'P3.2': deque(),
            'P3.3': deque(),
            'P3.4': deque()
        }

        # Incident deduplication
        self.incidents_emitted = defaultdict(int)

        # Start MonPoly monitors (REQUIRED - no fallback)
        self.monpoly_available = self._start_monitors()

        if not self.monpoly_available:
            print("❌ CRITICAL: MonPoly not available!", flush=True)
            print("   Layer 3 monitoring requires MonPoly for MFOTL evaluation.", flush=True)
            print("   System will not detect incidents without it.\n", flush=True)
            raise RuntimeError("MonPoly is required but not available")

        print("✅ Online MonPoly monitors started\n", flush=True)

    def _start_monitors(self):
        """Start persistent MonPoly processes for each property"""
        try:
            for prop_name, formula_file in self.properties.items():
                try:
                    # Start MonPoly in online mode (without -negate since our formulas output violations directly)
                    process = subprocess.Popen(
                        ['monpoly', '-sig', self.signature_file,
                         '-formula', formula_file],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1
                    )

                    self.monitors[prop_name] = process
                    self.monitor_status[prop_name] = True

                    # Start thread to read violations (stdout)
                    thread = threading.Thread(
                        target=self._read_violations,
                        args=(prop_name, process),
                        daemon=True
                    )
                    thread.start()
                    self.monitor_threads[prop_name] = thread

                    # Start thread to read errors (stderr)
                    error_thread = threading.Thread(
                        target=self._read_errors,
                        args=(prop_name, process),
                        daemon=True
                    )
                    error_thread.start()

                    print(f"Started online monitor for {prop_name}", flush=True)

                except FileNotFoundError as e:
                    print(f"⚠️  MonPoly not found for {prop_name}: {e}", flush=True)
                    self.monitor_status[prop_name] = False
                except Exception as e:
                    print(f"⚠️  Could not start monitor {prop_name}: {e}", flush=True)
                    self.monitor_status[prop_name] = False

            # Return True only if at least one monitor started
            return any(self.monitor_status.values())

        except Exception as e:
            print(f"⚠️  Could not start monitors: {e}", flush=True)
            return False

    def _read_violations(self, prop_name, process):
        """Thread to continuously read violations from MonPoly stdout"""
        try:
            for line in process.stdout:
                line = line.strip()
                #print(f"[MonPoly {prop_name} RAW] {line}", flush=True)  # ⭐
                if line and not line.startswith('#'):
                    self.violation_queues[prop_name].append(line)
        except Exception as e:
            print(f"⚠️  Error reading violations for {prop_name}: {e}", flush=True)
        finally:
            self.monitor_status[prop_name] = False
            print(f"⚠️  Monitor {prop_name} reader thread ended", flush=True)

    def _read_errors(self, prop_name, process):
        """Thread to continuously read errors/debug output from MonPoly stderr"""
        try:
            for line in process.stderr:
                line = line.strip()
                if line:
                    print(f"[{prop_name}] {line}", flush=True)
        except Exception as e:
            pass

    def _send_event(self, timestamp, predicate, device):
        """
        Send event to all MonPoly monitors incrementally
        Format: @timestamp predicate("device", timestamp)
        Binary predicates with explicit timestamp argument
        """
        ts_int = int(timestamp)
        event_line = f'@{ts_int} {predicate}("{device}", {ts_int})\n'

        for prop_name, process in self.monitors.items():
            # Skip if monitor is marked as dead
            if not self.monitor_status.get(prop_name, False):
                continue

            try:
                # Check if process is still running
                if process.poll() is not None:
                    # Process has terminated
                    self.monitor_status[prop_name] = False
                    print(f"⚠️  Monitor {prop_name} has terminated (exit code: {process.returncode})", flush=True)
                    continue

                # Process is alive, send event
                if process.stdin and not process.stdin.closed:
                    process.stdin.write(event_line)
                    process.stdin.flush()
            except BrokenPipeError:
                self.monitor_status[prop_name] = False
                print(f"⚠️  Broken pipe on {prop_name} - monitor process may have crashed", flush=True)
            except Exception as e:
                self.monitor_status[prop_name] = False
                print(f"⚠️  Error sending event to {prop_name}: {e}", flush=True)

    def _check_violations(self):
        """Check for new violations from all monitors."""
        all_incidents = []
        for prop_name in self.properties.keys():
            while self.violation_queues[prop_name]:
                violation = self.violation_queues[prop_name].popleft()
                incidents = self._parse_violation(violation, prop_name)
                all_incidents.extend(incidents)
        return all_incidents
    
    def _parse_violation(self, violation_line, property_name):
        """
        Parse MonPoly violation output and create incident(s).
        Returns a list of incidents (one per device found).
        """
        incidents = []
        try:
            if not violation_line.startswith('@'):
                return incidents

            parts = violation_line.split('.', 1)
            timestamp_str = parts[0].replace('@', '').strip()
            timestamp = int(timestamp_str)

            if ':' not in violation_line:
                return incidents

            output_section = violation_line.split(':', 1)[1].strip()

            if output_section == 'true':
                device_matches = []
            else:
                device_matches = re.findall(r'\(([^)]+)\)', output_section)

            if property_name == 'P3.1':
                # APT indicator: ένα incident ανά device
                for device in device_matches:
                    incident_key = f"apt_{device}_{timestamp//60}"
                    if self.incidents_emitted[incident_key] > 0:
                        continue
                    self.incidents_emitted[incident_key] += 1
                    incidents.append({
                        'type': 'apt_indicator',
                        'severity': 'CRITICAL',
                        'device': device,
                        'timestamp': timestamp,
                        'property': property_name,
                        'method': 'monpoly_online'
                    })

            elif property_name == 'P3.2':
                # Coordinated attack: CNT aggregation, output = (count)
                if not device_matches:
                    return incidents
                device_count = int(device_matches[0])
                incident_key = f"coordinated_{timestamp//30}"
                if self.incidents_emitted[incident_key] == 0:
                    self.incidents_emitted[incident_key] += 1
                    incidents.append({
                        'type': 'coordinated_attack',
                        'severity': 'CRITICAL',
                        'device_count': device_count,
                        'timestamp': timestamp,
                        'property': property_name,
                        'method': 'monpoly_online'
                    })

            elif property_name == 'P3.3':
                # Escalation pattern: ένα incident ανά device
                for device in device_matches:
                    incident_key = f"escalation_{device}_{timestamp//60}"
                    if self.incidents_emitted[incident_key] > 0:
                        continue
                    self.incidents_emitted[incident_key] += 1
                    incidents.append({
                        'type': 'escalation_pattern',
                        'severity': 'HIGH',
                        'device': device,
                        'timestamp': timestamp,
                        'property': property_name,
                        'method': 'monpoly_online'
                    })

            elif property_name == 'P3.4':
                # Persistent threat: CNT aggregation, output = (count, device) ανά match
                for match in device_matches:
                    parts = match.split(',')
                    if len(parts) < 2:
                        continue
                    try:
                        count = int(parts[0].strip())
                        device = parts[1].strip()
                    except ValueError:
                        continue
                    incident_key = f"persistent_{device}_{timestamp//3600}"
                    if self.incidents_emitted[incident_key] > 0:
                        continue
                    self.incidents_emitted[incident_key] += 1
                    incidents.append({
                        'type': 'persistent_threat',
                        'severity': 'HIGH',
                        'device': device,
                        'count': count,
                        'timestamp': timestamp,
                        'property': property_name,
                        'method': 'monpoly_online'
                    })

        except Exception as e:
            print(f"⚠️  Violation parse error: {e}", flush=True)
            print(f"    Line: {violation_line}", flush=True)

        return incidents
    # =========================================================================
    # Alert Processing
    # =========================================================================

    def process_alert(self, alert):
        """Process alert and evaluate all MFOTL properties"""
        device = alert['device']
        timestamp = alert['timestamp']
        alert_type = alert['type']

        # Map alert types to MonPoly predicates
        predicate_map = {
            'overflow': 'overflow',
            'time_anomaly': 'time_anomaly',
            'fuzzing': 'fuzzing',
            'dos_spam': 'dos_spam',
            'safe_tx': 'safe_tx'
        }

        if alert_type not in predicate_map:
            return []

        predicate = predicate_map[alert_type]

        # Send event to online MonPoly monitors
        self._send_event(timestamp, predicate, device)

        # Check for violations
        incidents = self._check_violations()

        return incidents

    def shutdown(self):
        """Cleanup: terminate MonPoly processes"""
        for prop_name, process in self.monitors.items():
            try:
                if self.monitor_status.get(prop_name, False):
                    if process.stdin and not process.stdin.closed:
                        process.stdin.close()
                    process.terminate()
                    process.wait(timeout=2)
            except Exception as e:
                print(f"⚠️  Error terminating {prop_name}: {e}", flush=True)


def emit_incident(incident):
    """Output incident"""
    severity_emoji = {
        'CRITICAL': '🔥🔥🔥',
        'HIGH': '🔥🔥',
        'MEDIUM': '🔥',
        'LOW': '⚠️'
    }
    emoji = severity_emoji.get(incident['severity'], '⚠️')

    print(f"\n{emoji} [INCIDENT DETECTED] {incident['type'].upper()}", flush=True)
    print(f"    Severity: {incident['severity']}", flush=True)
    print(f"    Property: {incident['property']}", flush=True)
    print(f"    Method: {incident.get('method', 'N/A')}", flush=True)

    if 'device' in incident:
        print(f"    Device: {incident['device']}", flush=True)
    if 'device_count' in incident:
        print(f"    Device Count: {incident['device_count']}", flush=True)

    print(f"    Details: {json.dumps(incident, indent=6)}\n", flush=True)

    try:
        with open("/shared_data/incidents.log", "a") as f:
            f.write(json.dumps(incident) + "\n")
    except Exception as e:
        print(f"    ⚠️  Could not write to incidents.log: {e}", flush=True)


def main():
    """Main monitoring loop"""

    try:
        monpoly_monitor = OnlineMonPolyMonitor()
        # tessla_monitor = OnlineTeSSLaMonitor()
        rtlola_monitor = OnlineRTLolaMonitor()
    except Exception as e:
        print(f"✗ Failed to initialize engines: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    alert_count = 0
    incident_count = 0

    try:
        for line in sys.stdin:
            line = line.strip()

            if not line or not line.startswith('{'):
                continue

            try:
                alert = json.loads(line)
                alert_count += 1

                print(f"[Layer3] Alert #{alert_count}: {alert['type']} from {alert['device']} @ t={alert['timestamp']}",
                      flush=True)

                # MonPoly
                monpoly_incidents = monpoly_monitor.process_alert(alert)
                for incident in monpoly_incidents:
                    incident_count += 1
                    emit_incident(incident)

                # TeSSLA
                #tessla_incident = tessla_monitor.process_alert(alert)
                #if tessla_incident:  # Το TeSSLa επιστρέφει single dict ή None
                #    incident_count += 1
                #    emit_incident(tessla_incident)

                rtlola_incidents = rtlola_monitor.process_alert(alert)
                for incident in rtlola_incidents:
                    incident_count +=1
                    emit_incident(incident)

            except (json.JSONDecodeError, KeyError) as e:
                print(f"[Layer3] ⚠️  Parse error: {e}", flush=True)
                continue

    except KeyboardInterrupt:
        pass
    
    # Grace period για pending RTLola violations
    print("[Layer3] Input closed, draining for 7s...", flush=True)
    deadline = time.time() + 7.0
    while time.time() < deadline:
        time.sleep(0.5)
        for incident in rtlola_monitor.get_pending_incidents():
            incident_count += 1
            emit_incident(incident)
        for incident in monpoly_monitor._check_violations():
            incident_count += 1
            emit_incident(incident)

    print(f"\n[Layer3] Shutting down...", flush=True)
    monpoly_monitor.shutdown()
    rtlola_monitor.shutdown()

if __name__ == "__main__":
    main()
