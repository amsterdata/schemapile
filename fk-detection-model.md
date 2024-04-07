# Foreign Key Detection Model

## starcoder-schemapile
The foreign key detection model is based on the 
[bigcode/starcoder (huggingface)](https://huggingface.co/bigcode/starcoder) model.

We provide the finetuned model in huggingface transformers format below: 

[starcoder-schemapile-fk (HuggingFace)](https://huggingface.co/tdoehmen/starcoder-schemapile-fk)

## t5-base-schemapile

We also provide a model based on the [t5-base](https://huggingface.co/t5-base) model, which was trained on the SchemaPile dataset.

[t5-schemapile-fk (HuggingFace)](https://huggingface.co/tdoehmen/t5-schemapile-fk)

## Valentine Benchmark

Implementation and evaluation code for the [valentine-schemapile-ensemble](experiments/foreign_key_detection/valentine-schemapile-ensemble) method.

