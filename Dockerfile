FROM astrocrpublic.azurecr.io/runtime:3.1-13

#####GEMINI GEROU ESSA PARTE#####
# 1. Começa como root para ter permissão de instalar pacotes
USER root
RUN pip install pyspark==3.5.0
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# 2. Instala o locale (Debian/Ubuntu style)
RUN apt-get update && apt-get install -y locales && \
    sed -i -e 's/# pt_BR.UTF-8 UTF-8/pt_BR.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen

# 3. Define as variáveis de ambiente
ENV LC_ALL pt_BR.UTF-8
ENV LANG pt_BR.UTF-8
ENV LANGUAGE pt_BR.UTF-8

# 4. VOLTA PARA O USUÁRIO PADRÃO DO ASTRO (importante!)
USER astro
RUN pip install pyspark==3.5.0