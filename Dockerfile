FROM python:3.11 as yarrow

WORKDIR /app
COPY yarrow /tmp/yarrow/yarrow
COPY pyproject.toml /tmp/yarrow/
COPY poetry.lock /tmp/yarrow/
COPY README.md /tmp/yarrow/
COPY LICENCE.md /tmp/yarrow/
RUN pip install -e /tmp/yarrow

CMD yarrow