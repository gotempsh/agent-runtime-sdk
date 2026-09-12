FROM node:22-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssh-server \
    && npm install --global @openai/codex@0.146.0 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash agent \
    && mkdir -p /run/sshd /workspace /home/agent/.ssh /home/agent/.codex \
    && chown -R agent:agent /workspace /home/agent/.ssh /home/agent/.codex \
    && chmod 700 /home/agent/.ssh /home/agent/.codex \
    && sed -i 's/^#PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config \
    && printf '\nPermitRootLogin no\nPubkeyAuthentication yes\n' >> /etc/ssh/sshd_config

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D", "-e"]
