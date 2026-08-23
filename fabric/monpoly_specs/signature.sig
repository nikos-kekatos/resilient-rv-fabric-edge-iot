# Alerts (Input) - Binary predicates: (device, timestamp)
overflow(string, int)
time_anomaly(string, int)
fuzzing(string, int)
dos_spam(string, int)
safe_tx(string, int)

# Incidents (Output) - Include timestamp for temporal context
apt_indicator(string, int)
escalation_pattern(string, int)
coordinated_attack(int, int)
persistent_threat(string, int)