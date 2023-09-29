# Analog of Celery. But with separate queue for every operator and with pydantic verification of input and output.

# Usage example
## Create new process:
    1. Create new python file.
    2. Create new class with parent class yarrow.operator.Operator:
       - defile field input (from pydantic.BaseModel)
       - define field output (from pydantic.BaseModel)
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
    5. Run `yarrow` command.

## Put new message to queue with name of your operator with
- body - is json with args, e.g. `{"a": 123, "b": "some string"}`
- reply_to - name of queue for answer of yarrow
- correlation_id - id of your new message

# Run unittests
- `pytest tests/`

# Answer statuses:
- If there is no reply_to property in message, then answer with status ERROR will send to queue __dead_letters_queue__.
- If there is ok: answer will send to queue reply_to with the same correlation_id and status DONE.
- If there is error: answer will send to queue reply_to with the same correlation_id and status ERROR.

# Model of answer:
- [correlation_id in properties]
- request - origin request object
- result - result of execute operator
- error - string of error text
- status - one of DONE, PROCESSING, ERROR. If the message in sequence is last, then status is DONE, PROCESSING otherwise.
- num - number of answer (if one answer - there is should "0" every time)

# Stream as answer
- When there is several messages in answer then every message will have it's num, and will have status "PROCESSING".
After this messages there will be message with status "DONE", num is the max value of this sequence and null at field
result.

# Examples of usage in folder "example".

# All operator.run function should be a generator, e.g. you need to use yield, not return.
Client will get 2 message: one with result and status "PROCESSING", and second with null
result and status "DONE".