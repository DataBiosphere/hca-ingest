## Builds the hca-ingest dev env image

# This is the Dagster user code deployment image
# Dockerfile is located in orchestration/Dockerfile
# FROM us.gcr.io/broad-dsp-gcr-public/monster-hca-dagster:682b3fc
FROM us.gcr.io/broad-dsp-gcr-public/monster-hca-dagster:WIP_bhill

ENV LANG='en_US.UTF-8' \
    LANGUAGE='en_US:en' \
    LC_ALL='en_US.UTF-8' \
    SBT_VERSION=1.7.1

# Install some helpful tools not included in the base image, as well as set up for JDK install
RUN apt-get update  \
    && DEBIAN_FRONTEND=noninteractive \
    && apt-get install -yqq --no-install-recommends \
    ca-certificates \
    curl \
    fontconfig \
    git \
    locales \
    sudo \
    tzdata \
    unzip \
    vim \
    && echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

# Install JDK
# based on https://github.com/AdoptOpenJDK/openjdk-docker/blob/master/11/jdk/debian/Dockerfile.hotspot.releases.full
ENV JAVA_VERSION jdk-11.0.11+9

#support for multiple architectures
RUN set -eux; \
    ARCH="$(dpkg --print-architecture)"; \
    case "${ARCH}" in \
       aarch64|arm64) \
         ESUM='4966b0df9406b7041e14316e04c9579806832fafa02c5d3bd1842163b7f2353a'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_aarch64_linux_hotspot_11.0.11_9.tar.gz'; \
         ;; \
       armhf|armv7l) \
         ESUM='2d7aba0b9ea287145ad437d4b3035fc84f7508e78c6fec99be4ff59fe1b6fc0d'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_arm_linux_hotspot_11.0.11_9.tar.gz'; \
         ;; \
       ppc64el|ppc64le) \
         ESUM='945b114bd0a617d742653ac1ae89d35384bf89389046a44681109cf8e4f4af91'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_ppc64le_linux_hotspot_11.0.11_9.tar.gz'; \
         ;; \
       s390x) \
         ESUM='5d81979d27d9d8b3ed5bca1a91fc899cbbfb3d907f445ee7329628105e92f52c'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_s390x_linux_hotspot_11.0.11_9.tar.gz'; \
         ;; \
       amd64|x86_64) \
         ESUM='e99b98f851541202ab64401594901e583b764e368814320eba442095251e78cb'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.11_9.tar.gz'; \
         ;; \
       *) \
         echo "Unsupported arch: ${ARCH}"; \
         exit 1; \
         ;; \
    esac; \
    curl -LfsSo /tmp/openjdk.tar.gz ${BINARY_URL}; \
    echo "${ESUM} */tmp/openjdk.tar.gz" | sha256sum -c -; \
    mkdir -p /opt/java/openjdk; \
    cd /opt/java/openjdk; \
    tar -xf /tmp/openjdk.tar.gz --strip-components=1; \
    rm -rf /tmp/openjdk.tar.gz;

ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:$PATH"

# Install sbt
RUN curl -L -o sbt-$SBT_VERSION.zip https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.zip \
    && unzip sbt-$SBT_VERSION.zip -d opt \
    && rm sbt-$SBT_VERSION.zip \
    && mv /opt/sbt /usr/local/bin/sbt

ENV PATH /usr/local/bin/sbt/bin:$PATH

ENV CLOUDSDK_PYTHON=/usr/local/bin/python

# Install gcloud CLI
# from https://cloud.google.com/sdk/docs/install#deb
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.gpg \
    && apt-get update -y \
    && apt-get install google-cloud-sdk -y
# note that your credentials will not be stored in this image, so you'll need to run
# "gcloud auth login" to authenticate with gcloud with your Broad credentials
# The set up your billing project "gcloud config set project PROJECT_ID"
# You should also run “gcloud auth application-default login” after installation
# and authenticate with your Broad credentials to set a default login for applications
#

# copy in the rest of the codebase & move contents of base image to orchestration
COPY . /hca-ingest/.
RUN mkdir /orchestration
RUN mv /hca_manage /orchestration/. \
    && mv /hca_orchestration /orchestration/. \
    && mv dagster.yaml /orchestration/. \
    && mv Dockerfile /orchestration/. \
    && mv mypy.ini /orchestration/. \
    && mv poetry.lock /orchestration/. \
    && mv pyproject.toml /orchestration/. \
    && mv pytest.ini /orchestration/. \
    && mv README.md /orchestration/. \
    && mv schema.json /orchestration/. \
    && mv /orchestration /hca-ingest/. \
    && cd /hca-ingest/
# NB sbt test and pytest require that you gcloud auth credentials, so they must be run in the container.

CMD ["bin/bash"]

# docker build -t test_hca_dev_env_local:0.1 .
# docker run --rm -it test_hca_dev_env_local:0.1
