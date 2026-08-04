import sys
import json
import time

def process_stream():
    for line in sys.stdin:
        line = line.strip()
        if not line.startswith('{'):
            continue
            
        try:
            event = json.loads(line)
            t, actor, tool, args = event["turn"], event["actor"], event["tool"], event["args"]
            
            is_overflow = args["actual_sent"] > args["buffer_limit"]
            is_fuzzed = args["actual_sent"] <= 0

            curr = int(time.time())
            is_time_spoofed = abs(curr - t) > 15
            
            
            if is_overflow and tool == "vulnerable_send":
                print(f"@{t} buffer_overflow(\"{actor}\", {args['actual_sent']})", flush=True)
            elif is_time_spoofed:
                print(f"@{curr} time_anomaly(\"{actor}\", {t})", flush=True)
            elif is_fuzzed:
                print(f"@{t} logic_fuzz_anomaly(\"{actor}\", {args['actual_sent']})", flush=True)
            else:
                print(f"@{t} safe_tx(\"{actor}\", {args['actual_sent']})", flush=True)
                
        except (json.JSONDecodeError, KeyError):
            continue

if __name__ == "__main__":
    process_stream()