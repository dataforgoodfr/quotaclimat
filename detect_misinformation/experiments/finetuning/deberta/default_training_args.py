DEFAULT_TRAINING_ARGS = dict(
    per_device_train_batch_size=4,
    per_device_eval_batch_size=2,
    learning_rate=5e-6,
    warmup_steps=50,
    weight_decay=0.01,
    lr_scheduler_type="cosine",
    num_train_epochs=10,
    bf16=False,  # bfloat16 training
    fp16=False,
    optim="adamw_torch_fused",  # improved optimizer
    # logging & evaluation strategies
    logging_strategy="steps",
    logging_steps=20,
    eval_strategy="steps",
    eval_steps=20,
    save_strategy="steps",
    save_steps=100,
    save_total_limit=2,
    load_best_model_at_end=True,
    use_mps_device=True,
    metric_for_best_model="f1",
)
