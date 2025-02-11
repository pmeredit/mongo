FROM public.ecr.aws/lambda/python:3.13

# Copy requirements.txt
COPY function/requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Copy function code
COPY function/lambda_function.py ${LAMBDA_TASK_ROOT}

ENV AWS_ACCESS_KEY_ID=myAccessKey
ENV AWS_SECRET_ACCESS_KEY=myAccessSecret
ENV AWS_SESSION_TOKEN=mySessionToken

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.handler" ]
