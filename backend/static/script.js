let history = [];

// Checks whether the backend model is loaded/ready and updates the status indicator in the UI
async function checkModelStatus() {
  try {
    const res = await fetch("/model/status");
    const data = await res.json();

    const modelStatus = document.getElementById("modelStatus");

    console.log("Model status:", data);

    if (data.available === true) {
      modelStatus.innerHTML = "Model Ready ✅";
    } else {
      modelStatus.innerHTML = "Model Not Ready ❌";
    }
  } catch (error) {
    // Fired if the request itself fails (e.g. server down, network issue)

    document.getElementById("modelStatus").innerHTML = "Backend Error ❌";
  }
}

// Main handler triggered when the user submits text for sentiment analysis
async function analyzeText() {
  const text = document.getElementById("textInput").value.trim();

  if (!text) {
    alert("Please enter a Reddit comment first.");
    return;
  }

// Re-check model status before every analysis in case it went offline
  await checkModelStatus();

  document.getElementById("emoji").innerHTML = "⏳";
  document.getElementById("prediction").innerHTML = "Analyzing...";
  document.getElementById("prediction").className = "prediction";

  try {
    const res = await fetch("/analyze", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        keyword: "",
        text: text,
      }),
    });

    const data = await res.json();

    console.log("Analyze response:", data);

    if (!res.ok) {
      document.getElementById("emoji").innerHTML = "⚠️";
      document.getElementById("prediction").innerHTML =
        data.detail || "Prediction Unavailable";

      addHistory(text, "unavailable", "-", new Date().toLocaleTimeString());
      return;
    }
   // Normalize the raw label from the backend into a consistent value
    
    const label = normalizeLabel(data.label);
    const confidence = getConfidence(label, data.proba);
    const eventTime = data.event_time || new Date().toLocaleTimeString();
    updatePredictionUI(label);
    addHistory(text, label, confidence, eventTime);

    document.getElementById("textInput").value = "";
  } catch (error) {
    document.getElementById("emoji").innerHTML = "❌";
    document.getElementById("prediction").innerHTML = "Connection Error";
    addHistory(text, "error", "-", new Date().toLocaleTimeString());
  }
}

function normalizeLabel(label) {
  const value = (label || "").toLowerCase();

  if (value.includes("positive")) {
    return "positive";
  }

  if (value.includes("negative")) {
    return "negative";
  }

  return "unknown";
}

function updatePredictionUI(label) {
  const emoji = document.getElementById("emoji");
  const prediction = document.getElementById("prediction");

  prediction.className = "prediction";

  if (label === "positive") {
    emoji.innerHTML = "😊";
    prediction.innerHTML = "POSITIVE";
    prediction.classList.add("positive");
  } else if (label === "negative") {
    emoji.innerHTML = "😡";
    prediction.innerHTML = "NEGATIVE";
    prediction.classList.add("negative");
  } else {
    emoji.innerHTML = "⚠️";
    prediction.innerHTML = "UNKNOWN";
  }
}

function getConfidence(label, proba) {
  if (!proba || Object.keys(proba).length === 0) {
    return "-";
  }

  if (proba[label] !== undefined) {
    return Math.round(proba[label] * 100) + "%";
  }

  let maxValue = 0;

  Object.keys(proba).forEach((key) => {
    if (proba[key] > maxValue) {
      maxValue = proba[key];
    }
  });

  return Math.round(maxValue * 100) + "%";
}

// Adds a new entry to the in-memory history list (capped at 10 items)
// and re-renders the history table
 function addHistory(text, label, confidence, eventTime) {
  history.unshift({
    text,
    label,
    confidence,
    eventTime,
  });

  if (history.length > 10) {
    history.pop();
  }

  renderHistoryTable();
}

function renderHistoryTable() {
  const table = document.getElementById("historyTable");
  table.innerHTML = "";

  if (history.length === 0) {
    table.innerHTML = `
      <tr>
        <td colspan="4">No analysis yet</td>
      </tr>
    `;
    return;
  }

  history.forEach((item) => {
    const shortText =
      item.text.length > 80 ? item.text.substring(0, 80) + "..." : item.text;

    table.innerHTML += `
      <tr>
        <td>${escapeHtml(shortText)}</td>
        <td class="${item.label}">${item.label.toUpperCase()}</td>
        <td>${item.confidence}</td>
        <td>${item.eventTime}</td>
      </tr>
    `;
  });
}

function escapeHtml(text) {
  const div = document.createElement("div");
  div.innerText = text;
  return div.innerHTML;
}

checkModelStatus();
renderHistoryTable();
