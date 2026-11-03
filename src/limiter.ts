export function createBrowserLimiter(Limiter, ...args) {
  const requests = new WeakMap();
  const tracked = (req) => req?.parsed?.searchParams.has("trackingId");
  return new (class extends Limiter {
    constructor(...options: any[]) {
      super(...options);
    }
    push(job) {
      const [req, socket] = job.args;
      if (tracked(req)) {
        // Admission may yield to health checks after the client has disconnected.
        if (socket.destroyed || socket.readableEnded) return this.length;
        const request = requests.get(socket);
        if (request) request.job = job;
      }
      return super.push(job);
    }
    limit(...options) {
      const limited = super.limit(...options);
      return (...args) => {
        const [req, socket] = args;
        if (!tracked(req)) return limited(...args);
        const aborted = Promise.withResolvers();
        const request: { job?: any } = {};
        requests.set(socket, request);
        const cancel = () => {
          const index = this.indexOf(request.job);
          if (request.job && index < 0) return; // Browserless still owns running requests.
          if (index >= 0) this.splice(index, 1);
          socket.destroy();
          aborted.reject(Error("browser_launch_cancelled"));
        };
        socket.once("close", cancel);
        socket.once("end", cancel);
        const pending = limited(...args);
        if (socket.destroyed || socket.readableEnded) cancel();
        return Promise.race([pending, aborted.promise]).finally(() => {
          socket.off("close", cancel);
          socket.off("end", cancel);
          requests.delete(socket);
        });
      };
    }
  })(...args);
}
