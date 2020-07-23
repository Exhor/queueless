FROM python:3.7-stretch as build
ENV PIP_DISABLE_PIP_VERSION_CHECK=on
RUN pip install poetry
WORKDIR /app
COPY poetry.lock pyproject.toml /app/
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction
COPY . /app

# TODO: slimmer docker image using distroless, e.g.:
#FROM gcr.io/distroless/python3-debian10 as qless-worker
#COPY --from=build /usr/local/lib/python3.7/site-packages/ /usr/local/lib/python3.7/site-packages/
#COPY --from=build /usr/local/lib/python3.7/ /usr/local/lib/python3.7/
#COPY --from=build /app/ /app/
EXPOSE 5000
ENV PYTHONPATH=/app/

ENTRYPOINT ["python", "-m", "worker"]