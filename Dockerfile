FROM python:3.11 as yarrow

WORKDIR /app
RUN pip install poetry==1.6.1
RUN poetry config virtualenvs.create false
RUN poetry config installer.modern-installation false
COPY pyproject.toml ./
COPY poetry.lock ./
RUN poetry install --without dev --no-root
COPY yarrow yarrow
COPY example example
COPY README.md ./
COPY LICENCE.md ./
RUN poetry install --without dev

CMD poetry run yarrow


FROM python:3.11 as integration-tests

WORKDIR /app

COPY integration_tests/requirements.txt .
RUN pip install -r requirements.txt
COPY integration_tests/ .

CMD pytest