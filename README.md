# Analog of Celery. But with separate queue for every operator and with pydantic verification of input and output.

# Usage example
## Create new process:
1. Create new python file.
2. Create new class with parent class yarrow.operator.Operator:
    - defile field input
    - define field output
    - write method "run"
3. Define envs:
    - HOST  # of rabbitmq
    - PORT  # of rabbitmq
    - VIRTUAL_HOST  # of rabbitmq
    - USERNAME  # of rabbitmq
    - PASSWORD  # of rabbitmq
    - CONFIG_FILENAME  # file with operators names
4. Install `yarrow`
    - pip install git+https://github.com/dmitriy-shikhalev/yarrow
4. Run `yarrow` command.

## Put new message to queue with name of your operator with
- body - is json with args, e.g. `{"a": 123, "b": "some string"}`
- reply_to - name of queue for answer of yarrow
- correlation_id - id of your new message

# Run unittests
- `pytest tests/`