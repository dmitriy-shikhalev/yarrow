FROM python:3.11 as yarrow

WORKDIR /app
COPY yarrow yarrow
COPY example/config.yaml ./example/config.yaml
COPY example/example.py ./example/example.py
COPY pyproject.toml ./
COPY poetry.lock ./
COPY README.md ./
COPY LICENCE.md ./
RUN pip install -e .

CMD yarrow