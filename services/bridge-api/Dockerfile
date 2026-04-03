FROM node:20-alpine AS builder

WORKDIR /app
COPY services/bridge-api/package*.json ./
RUN npm ci
COPY services/bridge-api/tsconfig.json ./
COPY services/bridge-api/src ./src
RUN npm run build

FROM node:20-alpine AS runtime

WORKDIR /app
COPY services/bridge-api/package*.json ./
RUN npm ci --omit=dev
COPY --from=builder /app/dist ./dist
COPY proto/bridge.proto /app/proto/bridge.proto

EXPOSE 8080
CMD ["node", "dist/index.js"]
