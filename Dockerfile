ARG PYTHON=3.13
FROM python:$PYTHON-slim-bookworm

ARG LMDBM=0.0.6
RUN apt update && apt -y install build-essential && \
	pip install lmdbm==$LMDBM pytablewriter genutility rich && \
	apt purge --auto-remove --yes build-essential && apt clean && \
	rm --recursive --force /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV PYTHONUNBUFFERED=1
COPY benchmark.py /
ENTRYPOINT [ "python", "benchmark.py" ]
