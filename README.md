# HPE Montor CLI Tool


### Poller commands

```bash
# Without FD/IO (API metrics only, runs as your user)
python -m poller --interval 15

# With FD/IO (full metrics, needs venv python under sudo)
sudo venv/bin/python -m poller --interval 15

# Faster polling, see output live
python -m poller --interval 5 --verbose


```