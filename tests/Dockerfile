FROM python:3.7-stretch as e2e
WORKDIR /test
COPY test_e2e.py /test/
RUN pip install queueless
RUN python test_e2e.py 'postgres://postgres:test@localhost:5000/qless'