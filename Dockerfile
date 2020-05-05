FROM python:3.8-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=on
ENV PIP_NO_CACHE_DIR=off
ENV PYTHONDONTWRITEBYTECODE=true
ENV PYTHONFAULTHANDLER=true
ENV PYTHONUNBUFFERED=true

WORKDIR /code

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        gcc \
        libsasl2-dev \
        libldap2-dev \
        libssl-dev \
        graphviz \
        libgraphviz-dev \
        libpq-dev \
        make \
 && rm -rf /var/lib/apt/lists/*

COPY Makefile poetry.lock pyproject.toml /code/

RUN pip install --no-compile --upgrade pip \
 && pip install --no-compile poetry \
 && poetry config virtualenvs.create false \
 && poetry install --no-dev --no-interaction --no-ansi \
 && pip uninstall --yes poetry

COPY example_collection /collection
RUN echo "Hello, world" > /tmp/hello_world_book_for_sharing

COPY constellations /code/constellations

CMD ["make", "up"]