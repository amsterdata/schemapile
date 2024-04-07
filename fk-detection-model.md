# Foreign Key Detection Model

## starcoder-schemapile
The foreign key detection model is based on the 
[bigcode/starcoder (huggingface)](https://huggingface.co/bigcode/starcoder) model.

We provide the finetuned model in huggingface transformers format below: 

[starcoder-schemapile.tar.gz](https://figshare.com/s/766cd2980ba146f3d23a)

## Data Preparation
The code for training data preparation, based on the SchemaPile dataset, can be found here:

[experiments/foreign_key_detection/training_data_prep_schemapile.ipynb](experiments/foreign_key_detection/training_data_prep_schemapile.ipynb)

## Training

The code used for fine-tuning can be found here:
[experiments/foreign_key_detection/starcoder_finetune](experiments/foreign_key_detection/starcoder_finetune)

We used 4x A100 40GB GPUs with DeepSpeed ZeRO-3 offloading. 

Hyperparameters:
- learning_rate: 2.0e-05
- num_train_epochs: 3
- gradient_accumulation_steps: 8
- per_device_train_batch_size: 4
- bf16: true
- warmup_ratio: 0.03
- weight_decay: 0.0

## t5-base-schemapile

We also provide a model based on the [t5-base](https://huggingface.co/t5-base) model, which was trained on the SchemaPile dataset.

[t5-base-schemapile.zip](https://huggingface.co/tdoehmen/t5-schemapile-fk)

## Valentine Benchmark

Implementation and evaluation code for the [valentine-schemapile-ensemble](experiments/foreign_key_detection/valentine-schemapile-ensemble) method.

