# install other dependencies
pip install torch==1.13.1+cu116 torchvision==0.14.1+cu116 torchaudio==0.13.1 --extra-index-url https://download.pytorch.org/whl/cu116
pip install gdown
pip install tqdm==4.65.0
pip install transformers==4.28.1
pip install datasets==2.11.0
pip install huggingface-hub==0.13.4
pip install accelerate
pip install peft
pip install -i https://test.pypi.org/simple/ bitsandbytes
pip install wandb
pip install scipy
pip install markupsafe==2.0.1 --upgrade

export 'PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:512'
torchrun --nproc_per_node=4 starcoder_finetune/train.py config.yaml --deepspeed=deepspeed_z3_config_bf16.json

