FROM node:20-alpine AS builder
RUN corepack enable
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages/ui-shadcn/package.json ./packages/ui-shadcn/
COPY packages/ui-shadcn ./packages/ui-shadcn
RUN pnpm install --frozen-lockfile
RUN pnpm --filter mysql-queue-ui-shadcn run build


FROM node:20-alpine AS runner
WORKDIR /app
COPY --from=builder /app/packages/ui-shadcn/.next/standalone ./.next/standalone
COPY --from=builder /app/packages/ui-shadcn/public ./.next/standalone/packages/ui-shadcn/public
COPY --from=builder /app/packages/ui-shadcn/.next/static ./.next/standalone/packages/ui-shadcn/.next/static
EXPOSE 3000
CMD ["node", ".next/standalone/packages/ui-shadcn/server.js"]
