## Builds the hca-ingest dev env image

# This is the Dagster user code deployment image
# Dockerfile is located in orchestration/Dockerfile
FROM us.gcr.io/broad-dsp-gcr-public/monster-hca-dagster:latest

ENV LANG='en_US.UTF-8' \
    LANGUAGE='en_US:en' \
    LC_ALL='en_US.UTF-8' \
    SBT_VERSION=1.7.1

# Install some helpful tools not included in the base image, as well as set up for JDK install
# python-is-python3 makes python3 the default, to avoid issues with poetry
RUN apt-get update  \
    && DEBIAN_FRONTEND=noninteractive \
    && apt-get install -yqq --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    fontconfig \
    git \
    gnupg \
    locales \
    python-is-python3 \
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

# Install gcloud CLI
ENV CLOUDSDK_PYTHON=/usr/local/bin/python

# Install gcloud CLI
# from https://cloud.google.com/sdk/docs/install#deb
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update -y \
    && apt-get install google-cloud-sdk -y \
    && apt-get install google-cloud-sdk-gke-gcloud-auth-plugin -y \
    && apt-get install kubectl -y
# note that your credentials will not be stored in this image, so you'll need to run
# "gcloud auth login" to authenticate with gcloud with your Broad credentials
# Then set up your billing project "gcloud config set project PROJECT_ID"
# You should also run “gcloud auth application-default login” after installation
# and authenticate with your Broad credentials to set a default login for applications
#

# Copy in the rest of the codebase & move contents of base image to orchestration
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

# builds with GitHub Action "Main Validation and Release" ../.github/workflows/build-and-publish-main.yaml
# tag = us-east4-docker.pkg.dev/broad-dsp-monster-hca-dev/monster-dev-env/hca_ingest_compose_dev_env:${{steps.get-artifact-slug.outputs.slug}}, us-east4-docker.pkg.dev/broad-dsp-monster-hca-dev/monster-dev-env/hca_ingest_compose_dev_env:latest

# For local Dev
# docker build -t us-east4-docker.pkg.dev/broad-dsp-monster-hca-dev/monster-dev-env/hca_ingest_compose_dev_env:<new_version> .
# docker run --rm -it us-east4-docker.pkg.dev/broad-dsp-monster-hca-dev/monster-dev-env/hca_ingest_compose_dev_env:<new_version>

# to build and push to Artifact Registry
# make sure you are logged in to gcloud and that application default credentials are set (https://docs.google.com/document/d/1b03-YphH6Uac5huBopLYTYjzgDAlwS6qf-orMqaph64/edit?usp=sharing)
# set the VERSION field in update_docker_image.sh in this directory and then run the script to build and push
# NB - this can take a while, so be patient
# It may be that you are on the split VPN and/or trying to push over IPv6. Either turn off the VPN or turn of IPV6
  #on your router to speed this up.
