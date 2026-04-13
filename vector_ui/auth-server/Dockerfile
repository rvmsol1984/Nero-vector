FROM node:20-alpine

WORKDIR /app

RUN apk add --no-cache tini \
    && mkdir -p /data \
    && chown -R node:node /app /data

COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund --loglevel=error

COPY index.js      ./
COPY users.json    ./
COPY routes        ./routes
COPY services      ./services

USER node

ENV NODE_ENV=production \
    PORT=3006 \
    SESSION_DB_DIR=/data

EXPOSE 3006

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "index.js"]
