/** Embeddable viewer. Credentials are supplied by the host, never by remote page content. */
export class BrowserViewer extends HTMLElement {
  #socket;
  #pending;
  #request = 0;
  #frame;
  #human = false;
  #canAcquire = false;
  #connected = false;
  #generation;
  #composing = false;
  #elements;
  #events;
  #refreshTimer;
  #queue = [];
  #documentId;
  #lastStatus;
  #inputBlocked = false;
  #targets = [];
  #selectedTarget = "";
  #canResize = false;
  #resizeObserver;
  #resizeTimer;
  #desiredViewport;
  #sentViewport;
  #viewportPolicy = "follow";
  #authentication;
  #authSubmitted = false;

  connectedCallback() {
    if (this.#elements) return;
    this.innerHTML = `<section class="browser-viewer" aria-label="Remote browser" data-connected="false">
      <div class="tab-strip"><div class="tabs" role="tablist" aria-label="Browser tabs"><span class="tab-placeholder">Waiting for browser</span></div></div>
      <div class="navigation" aria-label="Browser toolbar">
        <button type="button" data-navigation="Back" aria-label="Back" title="Back" disabled><svg viewBox="0 0 24 24" aria-hidden="true"><path d="m12 5-7 7 7 7M5 12h14"/></svg></button>
        <button type="button" data-navigation="Forward" aria-label="Forward" title="Forward" disabled><svg viewBox="0 0 24 24" aria-hidden="true"><path d="m12 5 7 7-7 7M5 12h14"/></svg></button>
        <button type="button" data-navigation="reload" aria-label="Reload page" title="Reload" disabled><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 7v5h-5M19 11a7 7 0 1 0-2 6M20 12l-4-5"/></svg></button>
        <form class="address-form" aria-label="Page navigation"><svg class="site-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M4 12h16M12 4c5 5 5 11 0 16-5-5-5-11 0-16"/></svg><input class="url" aria-label="Page address" type="url" maxlength="2048" placeholder="Waiting for a page" readonly><button class="visually-hidden" type="submit" tabindex="-1">Go</button></form>
        <details class="control-menu">
          <summary aria-label="Browser session controls" title="Browser session controls"><span class="status" role="status">Connecting</span><svg viewBox="0 0 24 24" aria-hidden="true"><path d="m8 10 4 4 4-4"/></svg></summary>
          <div class="menu-panel">
            <p class="session-note">Watching this browser. Control requires host authorization and a confirmed automation stop.</p>
            <button type="button" data-action="acquire" disabled>Take over browser</button>
            <button type="button" data-action="release" disabled>Return to AI</button>
            <button type="button" data-action="renew" disabled>Extend control time</button>
            <button type="button" data-action="refresh" disabled>Refresh image</button>
            <button type="button" data-action="tab" disabled>Send Tab key</button>
            <details class="alternative"><summary>Insert text…</summary><form class="input"><label>Text to insert<textarea maxlength="2000" disabled></textarea></label><button type="submit" disabled>Insert text</button></form></details>
            <p class="hint">Click the page to type. Escape returns focus to the address bar.</p>
          </div>
        </details>
      </div>
      <section class="viewport" tabindex="0" role="group" aria-label="Remote page. Take control, then click to type. Press Escape to return to the toolbar.">
        <div class="empty"><strong>Waiting for your browser</strong><p>The page will appear when your agent starts browsing.</p></div>
        <textarea class="keyboard-capture" aria-label="Remote page keyboard" tabindex="-1" autocapitalize="off" autocomplete="off" spellcheck="false" disabled></textarea>
        <img hidden alt="Live remote webpage. Page content is streamed as an image.">
      </section>
      <dialog class="auth-dialog" aria-label="Browser sign in"><form class="auth-form" autocomplete="on"><h2>Sign in to continue</h2><p class="auth-origin"></p><p class="auth-note">Your agent is waiting. Entered values are sent to this page; the agent receives only the submission status.</p><div class="auth-fields"></div><div class="auth-options"></div><div class="auth-actions"><button type="button" class="auth-cancel">Cancel</button><button type="submit" class="auth-submit">Continue</button></div></form></dialog>
      <p class="address visually-hidden" aria-label="Current page address">No page selected</p>
      <p class="notice" role="status" aria-live="polite"></p>
    </section>`;
    const root = this.querySelector(".browser-viewer");
    this.#elements = {
      root,
      status: root.querySelector(".status"),
      tabs: root.querySelector(".tabs"),
      menu: root.querySelector(".control-menu"),
      sessionNote: root.querySelector(".session-note"),
      image: root.querySelector("img"),
      empty: root.querySelector(".empty"),
      address: root.querySelector(".address"),
      viewport: root.querySelector(".viewport"),
      text: root.querySelector(".input textarea"),
      form: root.querySelector("form.input"),
      keyboard: root.querySelector(".keyboard-capture"),
      navigation: root.querySelector(".navigation"),
      url: root.querySelector(".url"),
      notice: root.querySelector(".notice"),
      auth: root.querySelector(".auth-dialog"),
    };
    this.#events = new AbortController();
    const options = { signal: this.#events.signal };
    this.#resizeObserver = new ResizeObserver(([entry]) => {
      this.#measureViewport(entry.contentRect.width, entry.contentRect.height);
      clearTimeout(this.#resizeTimer);
      this.#resizeTimer = setTimeout(() => this.#syncViewport(), 120);
    });
    this.#resizeObserver.observe(this.#elements.viewport);
    root.addEventListener(
      "click",
      (event) => {
        const tab = event.target.closest("button[data-target-id]");
        if (tab && !tab.disabled) {
          this.#queue = [];
          this.#frame = undefined;
          this.#selectedTarget = tab.dataset.targetId;
          this.#send("selectTarget", { targetId: this.#selectedTarget });
          this.#markTab();
          return;
        }
        const button = event.target.closest("button[data-action]");
        if (button && !button.disabled) this.#elements.menu.open = false;
        if (button?.dataset.action === "tab" && !button.disabled) {
          this.#input({ op: "key", key: "Tab" });
          return;
        }
        if (button && !button.disabled)
          this.#send(
            button.dataset.action,
            button.dataset.action === "release"
              ? {
                  targetId: this.#frame?.targetId,
                  frameRevision: this.#frame?.frameRevision,
                }
              : {},
          );
      },
      options,
    );
    root.addEventListener(
      "pointerdown",
      (event) => {
        if (!this.#elements.menu.contains(event.target))
          this.#elements.menu.open = false;
      },
      options,
    );
    this.#elements.tabs.addEventListener(
      "keydown",
      (event) => {
        if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
        const tabs = [...this.#elements.tabs.querySelectorAll("button")];
        const current = tabs.indexOf(document.activeElement);
        if (current < 0 || !tabs.length) return;
        event.preventDefault();
        const index =
          event.key === "Home"
            ? 0
            : event.key === "End"
              ? tabs.length - 1
              : (current + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) %
                tabs.length;
        tabs[index].focus();
        tabs[index].click();
      },
      options,
    );
    this.#elements.image.addEventListener(
      "click",
      (event) => {
        const point = this.#point(event);
        if (!point) return;
        this.#elements.keyboard.focus({ preventScroll: true });
        this.#input({ op: "pointer", ...point });
      },
      options,
    );
    this.#elements.viewport.addEventListener(
      "wheel",
      (event) => {
        const point = this.#point(event);
        if (!point || !this.#human) return;
        event.preventDefault();
        if (this.#pending) return;
        const factor =
          event.deltaMode === 1
            ? 16
            : event.deltaMode === 2
              ? this.#frame.viewportHeight
              : 1;
        this.#input({
          op: "scroll",
          ...point,
          deltaX: Math.max(-2000, Math.min(2000, event.deltaX * factor)),
          deltaY: Math.max(-2000, Math.min(2000, event.deltaY * factor)),
        });
      },
      { ...options, passive: false },
    );
    this.#elements.viewport.addEventListener(
      "focus",
      () => {
        if (this.#human) this.#elements.keyboard.focus({ preventScroll: true });
      },
      options,
    );
    this.#elements.keyboard.addEventListener(
      "keydown",
      (event) => {
        if (!this.#human || event.isComposing) return;
        if (event.key === "Escape") {
          event.preventDefault();
          this.#input({ op: "key", key: "Escape" });
          this.#elements.url.focus();
          return;
        }
        if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "a") {
          event.preventDefault();
          this.#input({ op: "key", key: "SelectAll" });
          return;
        }
        if (event.ctrlKey || event.metaKey || event.altKey) return;
        if (
          [
            "Enter",
            "Tab",
            "Backspace",
            "Delete",
            "ArrowLeft",
            "ArrowRight",
            "ArrowUp",
            "ArrowDown",
            "Home",
            "End",
            "PageUp",
            "PageDown",
          ].includes(event.key)
        ) {
          event.preventDefault();
          this.#input({ op: "key", key: event.key, modifiers: event.shiftKey ? 8 : 0 });
        }
      },
      options,
    );
    const commitText = () => {
      if (this.#composing) return;
      const value = this.#elements.keyboard.value;
      this.#elements.keyboard.value = "";
      if (value) this.#input({ op: "text", text: value });
    };
    this.#elements.keyboard.addEventListener("input", commitText, options);
    this.#elements.keyboard.addEventListener(
      "compositionstart",
      () => {
        this.#composing = true;
      },
      options,
    );
    this.#elements.keyboard.addEventListener(
      "compositionend",
      () => {
        this.#composing = false;
        commitText();
      },
      options,
    );
    this.#elements.navigation.addEventListener(
      "click",
      (event) => {
        const button = event.target.closest("button[data-navigation]");
        if (!button || button.disabled) return;
        this.#input(
          button.dataset.navigation === "reload"
            ? { op: "reload" }
            : { op: "key", key: button.dataset.navigation },
        );
      },
      options,
    );
    this.#elements.root.querySelector(".address-form").addEventListener(
      "submit",
      (event) => {
        event.preventDefault();
        this.#input({ op: "navigate", url: this.#elements.url.value });
      },
      options,
    );
    this.#elements.text.addEventListener(
      "compositionstart",
      () => {
        this.#composing = true;
      },
      options,
    );
    this.#elements.text.addEventListener(
      "compositionend",
      () => {
        this.#composing = false;
      },
      options,
    );
    this.#elements.form.addEventListener(
      "submit",
      (event) => {
        event.preventDefault();
        if (this.#composing || !this.#elements.text.value) return;
        this.#input({ op: "text", text: this.#elements.text.value });
      },
      options,
    );
    const respondAuth = (response) => {
      if (
        !this.#authentication ||
        this.#authSubmitted ||
        this.#pending ||
        !this.#connected
      )
        return;
      this.#authSubmitted = true;
      this.#send("authRespond", {
        requestId: this.#authentication.requestId,
        response,
      });
      this.#elements.auth.querySelector("form").reset();
      for (const input of this.#elements.auth.querySelectorAll("input"))
        input.value = "";
      this.#update();
    };
    this.#elements.auth.querySelector("form").addEventListener(
      "submit",
      (event) => {
        event.preventDefault();
        if (this.#authentication?.options) return;
        const values = Object.fromEntries(
          [...this.#elements.auth.querySelectorAll("input")].map((input) => [
            input.name,
            input.value,
          ]),
        );
        respondAuth({ action: "submit", values });
      },
      options,
    );
    this.#elements.auth
      .querySelector(".auth-cancel")
      .addEventListener("click", () => respondAuth({ action: "cancel" }), options);
    this.#elements.auth.addEventListener(
      "cancel",
      (event) => {
        event.preventDefault();
        respondAuth({ action: "cancel" });
      },
      options,
    );
    this.#elements.auth.querySelector(".auth-options").addEventListener(
      "click",
      (event) => {
        const button = event.target.closest("button[data-option]");
        if (button)
          respondAuth({ action: "submit", selectedOption: button.dataset.option });
      },
      options,
    );
    if (this.hasAttribute("data-standalone")) {
      const token = location.hash.slice(1);
      history.replaceState(null, "", location.pathname);
      if (/^[a-f0-9]{64}$/.test(token))
        this.connect({
          endpoint: `${location.origin.replace(/^http/, "ws")}/view/socket`,
          token,
        });
      else this.#notice("Open a complete viewer link issued by your host.");
    }
  }
  disconnectedCallback() {
    this.#resizeObserver?.disconnect();
    clearTimeout(this.#resizeTimer);
    this.disconnect();
    this.#events?.abort();
    this.#elements = undefined;
  }

  connect({ endpoint, token, viewport = "follow" }) {
    this.disconnect();
    if (!this.#elements) this.connectedCallback();
    const url = new URL(endpoint);
    if (
      !["ws:", "wss:"].includes(url.protocol) ||
      !/^[a-f0-9]{64}$/.test(token) ||
      url.username ||
      url.password
    )
      throw Error("Invalid viewer connection");
    this.setViewport(viewport);
    const socket = (this.#socket = new WebSocket(url));
    this.#elements.status.textContent = "Connecting…";
    socket.addEventListener("open", () => {
      if (socket === this.#socket)
        socket.send(JSON.stringify({ type: "authenticate", token }));
    });
    socket.addEventListener("message", (event) => {
      if (socket === this.#socket) this.#receive(event.data);
    });
    socket.addEventListener("close", () => {
      if (socket !== this.#socket) return;
      this.#renderAuth(undefined);
      this.#connected = false;
      this.#human = false;
      this.#frame = undefined;
      clearTimeout(this.#pending?.timer);
      this.#pending = undefined;
      this.#queue = [];
      this.dispatchEvent(
        new CustomEvent("browser-viewer-state", {
          detail: { connected: false, control: "disconnected" },
          bubbles: true,
        }),
      );
      this.#elements.status.textContent = "Disconnected";
      this.#notice(
        "Connection closed. Input is disabled. Reopen an authorized viewer link to reconnect; actions are never replayed.",
      );
      this.#update();
    });
    socket.addEventListener("error", () => socket.close());
  }
  disconnect() {
    clearTimeout(this.#refreshTimer);
    if (this.#elements) this.#elements.image.onload = null;
    this.#socket?.close();
    this.#socket = undefined;
    clearTimeout(this.#pending?.timer);
    this.#pending = undefined;
    this.#connected = false;
    this.#human = false;
    this.#frame = undefined;
    this.#queue = [];
    this.#documentId = undefined;
    this.#lastStatus = undefined;
    this.#sentViewport = undefined;
    this.#canResize = false;
    this.#inputBlocked = false;
    if (this.#elements) {
      this.#renderAuth(undefined);
      this.#update();
    }
  }
  setViewport(viewport = "follow") {
    if (
      viewport !== "follow" &&
      (!viewport ||
        !Number.isInteger(viewport.width) ||
        !Number.isInteger(viewport.height) ||
        viewport.width < 1 ||
        viewport.height < 1 ||
        viewport.width > 3840 ||
        viewport.height > 2160)
    )
      throw Error("Invalid viewer viewport");
    this.#viewportPolicy =
      viewport === "follow"
        ? "follow"
        : { width: viewport.width, height: viewport.height };
    if (!this.#elements) return;
    const rect = this.#elements.viewport.getBoundingClientRect();
    this.#measureViewport(rect.width, rect.height);
    this.#syncViewport();
  }
  #measureViewport(width, height) {
    this.#desiredViewport =
      this.#viewportPolicy === "follow"
        ? {
            width: Math.max(1, Math.min(3840, Math.floor(width))),
            height: Math.max(1, Math.min(2160, Math.floor(height))),
          }
        : this.#viewportPolicy;
  }
  #syncViewport() {
    if (
      this.#authentication ||
      !this.#canResize ||
      !this.#connected ||
      this.#pending ||
      this.#queue.length ||
      !this.#desiredViewport ||
      !this.#generation ||
      !this.#selectedTarget
    )
      return;
    const key = JSON.stringify([
      this.#generation,
      this.#selectedTarget,
      this.#desiredViewport,
    ]);
    if (key === this.#sentViewport) return;
    this.#sentViewport = key;
    this.#frame = undefined;
    this.#send("resize", {
      ...this.#desiredViewport,
      targetId: this.#selectedTarget,
      generation: this.#generation,
    });
  }
  #markTab() {
    for (const tab of this.#elements.tabs.querySelectorAll("button")) {
      const selected = tab.dataset.targetId === this.#selectedTarget;
      tab.setAttribute("aria-selected", String(selected));
      tab.tabIndex = selected ? 0 : -1;
    }
  }
  #notice(text) {
    this.#elements.notice.textContent = text;
  }
  #renderAuth(prompt) {
    const dialog = this.#elements.auth;
    if (!prompt) {
      if (dialog.open) dialog.close();
      dialog.querySelector(".auth-fields").replaceChildren();
      dialog.querySelector(".auth-options").replaceChildren();
      this.#authentication = undefined;
      this.#authSubmitted = false;
      return;
    }
    if (this.#authentication?.requestId === prompt.requestId) {
      this.#authentication = prompt;
      return;
    }
    this.#renderAuth(undefined);
    this.#authentication = prompt;
    this.#notice("");
    dialog.querySelector(".auth-origin").textContent = prompt.origin;
    dialog.querySelector(".auth-submit").hidden = !!prompt.options;
    dialog.querySelector(".auth-fields").replaceChildren(
      ...prompt.fields.map((field) => {
        const label = document.createElement("label");
        label.textContent = field.label;
        const input = document.createElement("input");
        input.name = field.id;
        input.type = ["password", "email"].includes(field.type) ? field.type : "text";
        input.autocomplete =
          field.type === "password"
            ? "current-password"
            : field.type === "code"
              ? "one-time-code"
              : "username";
        input.required = field.required !== false;
        input.maxLength = 2000;
        input.spellcheck = false;
        if (field.type === "code") input.inputMode = "numeric";
        label.append(input);
        return label;
      }),
    );
    dialog.querySelector(".auth-options").replaceChildren(
      ...(prompt.options ?? []).map((option) => {
        const button = document.createElement("button");
        button.type = "button";
        button.dataset.option = option.id;
        button.textContent = option.label;
        return button;
      }),
    );
    dialog.showModal();
  }
  #update() {
    const ready = this.#connected && !this.#pending && !this.#queue.length;
    for (const element of this.#elements.auth.querySelectorAll("input,button"))
      element.disabled =
        !ready || this.#authSubmitted || this.#authentication?.phase === "submitting";
    this.#elements.root.dataset.connected = String(this.#connected);
    for (const tab of this.#elements.tabs.querySelectorAll("button"))
      tab.disabled = !ready;
    this.#elements.root.dataset.control = this.#human ? "human" : "automation";
    this.querySelector('[data-action="acquire"]').hidden = this.#human;
    for (const action of ["release", "renew", "refresh", "tab"])
      this.querySelector(`[data-action="${action}"]`).hidden = !this.#human;
    for (const button of this.querySelectorAll("button[data-action]"))
      button.disabled =
        !ready ||
        (button.dataset.action === "acquire"
          ? !this.#canAcquire || this.#human
          : !this.#human ||
            (button.dataset.action === "release" && !this.#frame?.frameRevision));
    this.#elements.keyboard.disabled =
      !this.#connected || !this.#human || this.#inputBlocked;
    this.#elements.url.readOnly = !ready || !this.#human;
    for (const button of this.#elements.navigation.querySelectorAll(
      "button[data-navigation], .address-form button",
    ))
      button.disabled = !ready || !this.#human || !this.#frame?.frameRevision;
    this.#elements.text.disabled = !ready || !this.#human;
    this.#elements.form.querySelector("button").disabled =
      !ready || !this.#human || !this.#frame?.frameRevision;
    this.#elements.viewport.setAttribute("aria-busy", String(!!this.#pending));
  }
  #send(type, payload = {}) {
    if (!this.#connected || this.#pending) return;
    const id = ++this.#request;
    const timer = setTimeout(() => {
      this.#pending = undefined;
      this.#frame = undefined;
      this.#queue = [];
      this.#inputBlocked = true;
      this.#notice(
        "No confirmed result. The action will not be retried. Refresh the view before continuing.",
      );
      this.#update();
    }, 32_000);
    this.#pending = { id, timer, type, action: payload.action?.op };
    this.#socket.send(JSON.stringify({ type, id, ...payload }));
    this.#update();
  }
  #input(action) {
    if (!this.#human || !this.#connected || this.#inputBlocked) return;
    const last = this.#queue.at(-1);
    if (
      action.op === "text" &&
      (typeof action.text !== "string" || action.text.length > 2000)
    ) {
      this.#notice("Text exceeds the 2,000 character limit. Use smaller passages.");
      return;
    }
    if (this.#queue.length >= 32) {
      this.#queue = [];
      this.#notice(
        "Input paused because the connection is too slow. Refresh before continuing.",
      );
      return;
    }
    if (
      action.op === "text" &&
      last?.op === "text" &&
      last.text.length + action.text.length <= 2000
    )
      last.text += action.text;
    else this.#queue.push(action);
    this.#flush();
  }
  #flush() {
    if (
      !this.#human ||
      !this.#connected ||
      this.#pending ||
      !this.#frame?.frameRevision ||
      !this.#queue.length
    )
      return;
    const frame = this.#frame;
    this.#frame = undefined;
    this.#send("input", {
      action: this.#queue.shift(),
      generation: this.#generation,
      targetId: frame.targetId,
      frameRevision: frame.frameRevision,
    });
  }
  #point(event) {
    if (!this.#human || !this.#frame || this.#pending) return;
    const image = this.#elements.image;
    const rect = image.getBoundingClientRect();
    if (!rect.width || !rect.height || !image.naturalWidth || !image.naturalHeight)
      return;
    const scale = Math.min(
      rect.width / image.naturalWidth,
      rect.height / image.naturalHeight,
    );
    const width = image.naturalWidth * scale,
      height = image.naturalHeight * scale;
    const x = event.clientX - rect.left - (rect.width - width) / 2;
    const y = event.clientY - rect.top - (rect.height - height) / 2;
    if (x < 0 || y < 0 || x >= width || y >= height) return;
    return {
      x: Math.min(
        this.#frame.viewportWidth - 1,
        (x / width) * this.#frame.viewportWidth,
      ),
      y: Math.min(
        this.#frame.viewportHeight - 1,
        (y / height) * this.#frame.viewportHeight,
      ),
    };
  }

  #receive(raw) {
    let message;
    try {
      message = JSON.parse(raw);
    } catch {
      return this.disconnect();
    }
    if (message.type === "state") {
      this.#connected = true;
      if (this.#generation !== message.generation) {
        this.#frame = undefined;
        this.#generation = message.generation;
      }
      this.#renderAuth(message.authentication);
      this.#canResize = message.canResize === true;
      this.#human = message.leasedByYou;
      if (!this.#human) this.#queue = [];
      const status = JSON.stringify({
        connected: true,
        control: message.control,
        leasedByYou: message.leasedByYou,
        leaseExpiresAt: message.leaseExpiresAt,
        browser: message.browser.state,
        generation: message.generation,
      });
      if (status !== this.#lastStatus) {
        this.#lastStatus = status;
        this.dispatchEvent(
          new CustomEvent("browser-viewer-state", {
            detail: JSON.parse(status),
            bubbles: true,
          }),
        );
      }
      this.#canAcquire =
        message.canAcquire && ["automation", "uncertain"].includes(message.control);
      this.#elements.status.textContent = this.#human
        ? "You have control"
        : message.control === "automation"
          ? "Viewing"
          : "Control paused";
      this.#elements.sessionNote.textContent = this.#human
        ? "You control this browser. Click the page to type or paste."
        : message.canAcquire
          ? "Automation can be paused so you can use this browser."
          : "Watching this browser. Control requires host authorization and a confirmed automation stop.";
      if (
        ["unused", "starting"].includes(message.browser.state) &&
        this.#elements.image.hidden
      )
        this.#elements.empty.querySelector("p").textContent =
          message.browser.state === "unused"
            ? "Waiting for the agent to use its browser. This view does not start it."
            : "Preparing the browser. This can take a moment.";
      if (["lost", "stopped", "expired"].includes(message.browser.state)) {
        this.#frame = undefined;
        this.#elements.image.hidden = true;
        this.#elements.empty.hidden = false;
        this.#elements.empty.querySelector("p").textContent =
          "The browser is no longer live. Your host must explicitly start or recover it.";
      }
    } else if (message.type === "targets") {
      this.#targets = message.targets;
      if (!this.#targets.some((target) => target.targetId === this.#selectedTarget))
        this.#selectedTarget = this.#targets[0]?.targetId ?? "";
      this.#elements.tabs.replaceChildren(
        ...this.#targets.map((target) => {
          const tab = document.createElement("button");
          tab.type = "button";
          tab.setAttribute("role", "tab");
          tab.dataset.targetId = target.targetId;
          tab.textContent = target.title || target.url;
          tab.title = target.title ? `${target.title} — ${target.url}` : target.url;
          return tab;
        }),
      );
      this.#markTab();
    } else if (message.type === "frame") {
      if (
        message.generation !== this.#generation ||
        typeof message.data !== "string" ||
        message.data.length > 8 * 1024 * 1024
      )
        return;
      if (!message.interactive && (this.#human || this.#pending)) {
        if (message.sequence)
          this.#socket.send(
            JSON.stringify({ type: "ack", sequence: message.sequence }),
          );
        return;
      }
      const image = this.#elements.image;
      image.onload = () => {
        if (
          message.interactive &&
          this.#documentId &&
          this.#documentId !== message.documentId
        )
          this.#queue = [];
        this.#documentId = message.documentId;
        if (message.interactive) this.#inputBlocked = false;
        this.#frame = message;
        image.hidden = false;
        this.#elements.empty.hidden = true;
        this.#selectedTarget = message.targetId;
        this.#markTab();
        this.#elements.address.textContent =
          this.#targets.find((target) => target.targetId === this.#selectedTarget)
            ?.url ?? "Remote page";
        if (
          this.#elements.url.readOnly ||
          document.activeElement !== this.#elements.url
        )
          this.#elements.url.value =
            this.#targets.find((target) => target.targetId === this.#selectedTarget)
              ?.url ?? "";
        if (message.sequence && this.#socket?.readyState === WebSocket.OPEN)
          this.#socket.send(
            JSON.stringify({ type: "ack", sequence: message.sequence }),
          );
        this.#update();
        this.#flush();
        clearTimeout(this.#refreshTimer);
        if (message.interactive)
          this.#refreshTimer = setTimeout(() => {
            if (
              this.#human &&
              this.#connected &&
              !this.#pending &&
              !this.#queue.length &&
              !this.#composing
            )
              this.#send("refresh");
          }, 1000);
      };
      image.src = `data:image/jpeg;base64,${message.data}`;
    } else if (message.type === "result" || message.type === "error") {
      const pending = this.#pending;
      if (pending && message.id === pending.id) {
        clearTimeout(pending.timer);
        this.#pending = undefined;
      }
      if (message.type === "error") {
        if (pending?.type === "authRespond") this.#authSubmitted = false;
        this.#frame = undefined;
        this.#queue = [];
        this.#inputBlocked = true;
        this.#notice(
          `${String(message.error)
            .replace(/^browser_/, "")
            .replaceAll("_", " ")}. Refresh the view if needed; no action was retried.`,
        );
      } else if (pending?.type === "authRespond") {
        this.#notice("Response sent. Your agent will check the page and continue.");
      } else if (pending?.type === "release") {
        this.#human = false;
        this.#frame = undefined;
        this.#notice(
          message.result.resumed
            ? "Control returned. Automation resumed."
            : "Control returned. Resume or reconnect your agent through the host.",
        );
      } else {
        if (pending?.action === "text") this.#elements.text.value = "";
        this.#notice("");
      }
      this.dispatchEvent(
        new CustomEvent("browser-viewer-result", {
          bubbles: true,
          detail: {
            type: message.type,
            requestId: message.id,
            result: message.result,
            error: message.error,
          },
        }),
      );
    }
    this.#update();
    this.#flush();
    this.#syncViewport();
  }
}
customElements.define("browser-viewer", BrowserViewer);
