(function () {
  const panel = document.querySelector(".listener-panel");
  if (!panel) return;

  const startButton = document.getElementById("listen-start");
  const stopButton = document.getElementById("listen-stop");
  const statusLabel = document.getElementById("listener-status");
  const meter = document.getElementById("audio-meter");

  let ws = null;
  let audioContext = null;
  let nextStartTime = 0;
  let active = false;

  function wsUrl() {
    const path = panel.dataset.wsUrl;
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${protocol}//${window.location.host}${path}`;
  }

  function setStatus(text) {
    statusLabel.textContent = text;
  }

  function updateMeter(samples) {
    let peak = 0;
    for (let i = 0; i < samples.length; i += 32) {
      peak = Math.max(peak, Math.abs(samples[i]));
    }
    meter.style.width = `${Math.min(100, Math.round(peak * 100))}%`;
  }

  function playPcmFrame(arrayBuffer) {
    if (!audioContext || arrayBuffer.byteLength < 4) return;
    const channels = 2;
    const sampleRate = 48000;
    const view = new DataView(arrayBuffer);
    const frameCount = Math.floor(arrayBuffer.byteLength / 4);
    const audioBuffer = audioContext.createBuffer(channels, frameCount, sampleRate);
    const left = audioBuffer.getChannelData(0);
    const right = audioBuffer.getChannelData(1);

    for (let i = 0; i < frameCount; i++) {
      left[i] = view.getInt16(i * 4, true) / 32768;
      right[i] = view.getInt16(i * 4 + 2, true) / 32768;
    }

    updateMeter(left);
    const source = audioContext.createBufferSource();
    source.buffer = audioBuffer;
    source.connect(audioContext.destination);

    const now = audioContext.currentTime;
    if (nextStartTime < now + 0.06) {
      nextStartTime = now + 0.06;
    }
    source.start(nextStartTime);
    nextStartTime += frameCount / sampleRate;
  }

  async function start() {
    if (ws) return;
    active = true;
    localStorage.setItem("kikisenbou.autostart", "1");
    startButton.classList.add("hidden");
    stopButton.classList.remove("hidden");
    setStatus("接続中");

    audioContext = audioContext || new AudioContext({ sampleRate: 48000 });
    await audioContext.resume();
    nextStartTime = audioContext.currentTime + 0.08;

    ws = new WebSocket(wsUrl());
    ws.binaryType = "arraybuffer";
    ws.onopen = () => {
      setStatus("再生中");
      ws.keepAlive = window.setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) ws.send("ping");
      }, 15000);
    };
    ws.onmessage = (event) => playPcmFrame(event.data);
    ws.onclose = () => {
      if (ws && ws.keepAlive) window.clearInterval(ws.keepAlive);
      ws = null;
      meter.style.width = "0%";
      if (active) {
        setStatus("再接続中");
        window.setTimeout(startReconnect, 1200);
      } else {
        setStatus("停止中");
      }
    };
    ws.onerror = () => setStatus("接続エラー");
  }

  function startReconnect() {
    if (!active || ws) return;
    startButton.classList.add("hidden");
    stopButton.classList.remove("hidden");
    start();
  }

  function stop() {
    active = false;
    localStorage.removeItem("kikisenbou.autostart");
    startButton.classList.remove("hidden");
    stopButton.classList.add("hidden");
    meter.style.width = "0%";
    if (ws) ws.close();
    if (audioContext) audioContext.suspend();
  }

  startButton.addEventListener("click", start);
  stopButton.addEventListener("click", stop);

  if (localStorage.getItem("kikisenbou.autostart") === "1") {
    start().catch(() => {
      setStatus("クリックして再開");
      startButton.classList.remove("hidden");
      stopButton.classList.add("hidden");
    });
  }
})();
