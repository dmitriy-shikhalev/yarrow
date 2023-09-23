FROM python:3.11 as yarrow

WORKDIR /app
COPY yarrow yarrow
COPY example example
COPY pyproject.toml ./
COPY poetry.lock ./
COPY README.md ./
COPY LICENCE.md ./
RUN pip install -e .

CMD yarrow


FROM python:3.11 as integration-tests

WORKDIR /app

COPY integration_tests/requirements.txt .
RUN pip install -r requirements.txt
COPY integration_tests/ .

CMD pytest