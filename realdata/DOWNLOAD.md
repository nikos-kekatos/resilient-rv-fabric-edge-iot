# Datasets for §7 (Real-Data Validation)

The two datasets are **not bundled** (size + licensing). Download them into the
sub-folders below, then run the scripts as in the top-level `README.md` §4.

## WUSTL-IIoT-2021 (water-supply SCADA, Argus/Modbus flows)

Openly published by Washington University in St. Louis (~1M Argus flows, port 502).

```bash
mkdir -p wustl && cd wustl
curl -kLO https://www.cse.wustl.edu/~jain/iiot2/ftp/wustl_iiot_2021.zip
unzip wustl_iiot_2021.zip          # -> wustl_iiot_2021.csv
cd ..
```
Landing page: https://www.cse.wustl.edu/~jain/iiot2/index.html
Expected file: `realdata/wustl/wustl_iiot_2021.csv`

## TON_IoT (IoT/IIoT, Zeek network flows)

Published by UNSW Canberra (Moustafa et al.). Use the **Train_Test_Network** CSV from the
"Processed/Network" split.

- Portal: https://research.unsw.edu.au/projects/toniot-datasets
Expected file: `realdata/ton_iot/Train_Test_Network.csv`

## Sanity check

```bash
ls -la wustl/wustl_iiot_2021.csv ton_iot/Train_Test_Network.csv
```

Both scripts accept `--max N` to run on a subset for a quick smoke test (e.g. `--max 50000`).
