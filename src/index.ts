export interface PoolOptions<T> {
  create: () => Promise<T>;
  destroy?: (resource: T) => Promise<void>;
  validate?: (resource: T) => Promise<boolean>;
  min?: number;
  max?: number;
  acquireTimeoutMs?: number;
}

interface Waiter<T> {
  resolve: (r: T) => void;
  reject: (e: Error) => void;
  timer: NodeJS.Timeout;
}

export class Pool<T> {
  private idle: T[] = [];
  private busy = new Set<T>();
  private waiting: Waiter<T>[] = [];
  private size = 0;
  private closed = false;

  constructor(private opts: PoolOptions<T>) {
    this.opts.min = opts.min ?? 0;
    this.opts.max = opts.max ?? 10;
    this.opts.acquireTimeoutMs = opts.acquireTimeoutMs ?? 30000;
  }

  async acquire(): Promise<T> {
    if (this.closed) throw new Error("Pool is closed");

    if (this.idle.length) {
      const r = this.idle.pop()!;
      if (this.opts.validate && !(await this.opts.validate(r))) {
        this.size--;
        if (this.opts.destroy) await this.opts.destroy(r);
        return this.acquire();
      }
      this.busy.add(r);
      return r;
    }

    if (this.size < (this.opts.max ?? 10)) {
      this.size++;
      try {
        const r = await this.opts.create();
        this.busy.add(r);
        return r;
      } catch (e) {
        this.size--;
        throw e;
      }
    }

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.waiting.findIndex((w) => w.timer === timer);
        if (idx >= 0) this.waiting.splice(idx, 1);
        reject(new Error("Acquire timeout"));
      }, this.opts.acquireTimeoutMs);
      this.waiting.push({ resolve, reject, timer });
    });
  }

  release(resource: T): void {
    if (!this.busy.has(resource)) return;
    this.busy.delete(resource);

    const waiter = this.waiting.shift();
    if (waiter) {
      clearTimeout(waiter.timer);
      this.busy.add(resource);
      waiter.resolve(resource);
      return;
    }

    this.idle.push(resource);
  }

  async drain(): Promise<void> {
    this.closed = true;
    for (const w of this.waiting) {
      clearTimeout(w.timer);
      w.reject(new Error("Pool draining"));
    }
    this.waiting = [];
    if (this.opts.destroy) {
      for (const r of this.idle) await this.opts.destroy(r);
    }
    this.idle = [];
  }

  stats() {
    return { size: this.size, idle: this.idle.length, busy: this.busy.size, waiting: this.waiting.length };
  }
}
