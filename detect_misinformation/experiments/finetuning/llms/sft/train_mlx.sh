#! /bin/bash

mlx_lm_lora.train --model mlx-community/SmolLM3-3B-4bit --train --data data_xml/ --train-mode lora --epoch 1 --use-chat-template --adapter-path smollm3_xml
