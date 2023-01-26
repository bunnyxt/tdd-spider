#!/bin/bash

ps -aux | grep -E 'python -u [0-9]+_[a-zA-Z0-9-]+\.py'
