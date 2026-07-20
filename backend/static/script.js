// let history = [];

// // Checks whether the backend model is loaded/ready and updates the status indicator in the UI
// async function checkModelStatus() {
//   try {
//     const res = await fetch("/model/status");
//     const data = await res.json();

//     const modelStatus = document.getElementById("modelStatus");

//     console.log("Model status:", data);

//     if (data.available === true) {
//       modelStatus.innerHTML = "Model Ready ✅";
//     } else {
//       modelStatus.innerHTML = "Model Not Ready ❌";
//     }
//   } catch (error) {
//     // Fired if the request itself fails (e.g. server down, network issue)

//     document.getElementById("modelStatus").innerHTML = "Backend Error ❌";
//   }
// }

// // Main handler triggered when the user submits text for sentiment analysis
// async function analyzeText() {
//   const text = document.getElementById("textInput").value.trim();

//   if (!text) {
//     alert("Please enter a Reddit comment first.");
//     return;
//   }

// // Re-check model status before every analysis in case it went offline
//   await checkModelStatus();

//   document.getElementById("emoji").innerHTML = "⏳";
//   document.getElementById("prediction").innerHTML = "Analyzing...";
//   document.getElementById("prediction").className = "prediction";

//   try {
//     const res = await fetch("/analyze", {
//       method: "POST",
//       headers: {
//         "Content-Type": "application/json",
//       },
//       body: JSON.stringify({
//         keyword: "",
//         text: text,
//       }),
//     });

//     const data = await res.json();

//     console.log("Analyze response:", data);

//     if (!res.ok) {
//       document.getElementById("emoji").innerHTML = "⚠️";
//       document.getElementById("prediction").innerHTML =
//         data.detail || "Prediction Unavailable";

//       addHistory(text, "unavailable", "-", new Date().toLocaleTimeString());
//       return;
//     }
//    // Normalize the raw label from the backend into a consistent value

//     const label = normalizeLabel(data.label);
//     const confidence = getConfidence(label, data.proba);
//     const eventTime = data.event_time || new Date().toLocaleTimeString();
//     updatePredictionUI(label);
//     addHistory(text, label, confidence, eventTime);

//     document.getElementById("textInput").value = "";
//   } catch (error) {
//     document.getElementById("emoji").innerHTML = "❌";
//     document.getElementById("prediction").innerHTML = "Connection Error";
//     addHistory(text, "error", "-", new Date().toLocaleTimeString());
//   }
// }

// function normalizeLabel(label) {
//   const value = (label || "").toLowerCase();

//   if (value.includes("positive")) {
//     return "positive";
//   }

//   if (value.includes("negative")) {
//     return "negative";
//   }

//   return "unknown";
// }

// function updatePredictionUI(label) {
//   const emoji = document.getElementById("emoji");
//   const prediction = document.getElementById("prediction");

//   prediction.className = "prediction";

//   if (label === "positive") {
//     emoji.innerHTML = "😊";
//     prediction.innerHTML = "POSITIVE";
//     prediction.classList.add("positive");
//   } else if (label === "negative") {
//     emoji.innerHTML = "😡";
//     prediction.innerHTML = "NEGATIVE";
//     prediction.classList.add("negative");
//   } else {
//     emoji.innerHTML = "⚠️";
//     prediction.innerHTML = "UNKNOWN";
//   }
// }

// function getConfidence(label, proba) {
//   if (!proba || Object.keys(proba).length === 0) {
//     return "-";
//   }

//   if (proba[label] !== undefined) {
//     return Math.round(proba[label] * 100) + "%";
//   }

//   let maxValue = 0;

//   Object.keys(proba).forEach((key) => {
//     if (proba[key] > maxValue) {
//       maxValue = proba[key];
//     }
//   });

//   return Math.round(maxValue * 100) + "%";
// }

// // Adds a new entry to the in-memory history list (capped at 10 items)
// // and re-renders the history table
//  function addHistory(text, label, confidence, eventTime) {
//   history.unshift({
//     text,
//     label,
//     confidence,
//     eventTime,
//   });

//   if (history.length > 10) {
//     history.pop();
//   }

//   renderHistoryTable();
// }

// function renderHistoryTable() {
//   const table = document.getElementById("historyTable");
//   table.innerHTML = "";

//   if (history.length === 0) {
//     table.innerHTML = `
//       <tr>
//         <td colspan="4">No analysis yet</td>
//       </tr>
//     `;
//     return;
//   }

//   history.forEach((item) => {
//     const shortText =
//       item.text.length > 80 ? item.text.substring(0, 80) + "..." : item.text;

//     table.innerHTML += `
//       <tr>
//         <td>${escapeHtml(shortText)}</td>
//         <td class="${item.label}">${item.label.toUpperCase()}</td>
//         <td>${item.confidence}</td>
//         <td>${item.eventTime}</td>
//       </tr>
//     `;
//   });
// }

// function escapeHtml(text) {
//   const div = document.createElement("div");
//   div.innerText = text;
//   return div.innerHTML;
// }

// checkModelStatus();
// renderHistoryTable();




"use strict";

const API_BASE_URL = "";
const REFRESH_INTERVAL = 3000;

const elements = {
    form: document.getElementById("comparisonForm"),

    firstInput: document.getElementById("firstInput"),
    secondInput: document.getElementById("secondInput"),

    firstCharacterCount: document.getElementById(
        "firstCharacterCount"
    ),
    secondCharacterCount: document.getElementById(
        "secondCharacterCount"
    ),

    compareButton: document.getElementById("compareButton"),
    stopButton: document.getElementById("stopButton"),

    buttonText: document.querySelector(
        "#compareButton .button-text"
    ),
    buttonLoader: document.querySelector(
        "#compareButton .button-loader"
    ),

    messageBox: document.getElementById("messageBox"),

    modelStatus: document.getElementById("modelStatus"),
    modelStatusText: document.getElementById(
        "modelStatusText"
    ),

    comparisonSummary: document.getElementById(
        "comparisonSummary"
    ),

    firstResultName: document.getElementById(
        "firstResultName"
    ),
    secondResultName: document.getElementById(
        "secondResultName"
    ),

    firstSentiment: document.getElementById(
        "firstSentiment"
    ),
    secondSentiment: document.getElementById(
        "secondSentiment"
    ),

    firstPositive: document.getElementById(
        "firstPositive"
    ),
    firstNegative: document.getElementById(
        "firstNegative"
    ),

    secondPositive: document.getElementById(
        "secondPositive"
    ),
    secondNegative: document.getElementById(
        "secondNegative"
    ),

    comparisonTitle: document.getElementById(
        "comparisonTitle"
    ),
    comparisonWinner: document.getElementById(
        "comparisonWinner"
    ),
    positiveDifference: document.getElementById(
        "positiveDifference"
    ),
    differenceBar: document.getElementById(
        "differenceBar"
    ),

    liveStatus: document.getElementById("liveStatus"),

    firstTableHeader: document.getElementById(
        "firstTableHeader"
    ),
    secondTableHeader: document.getElementById(
        "secondTableHeader"
    ),

    historyTableBody: document.getElementById(
        "historyTableBody"
    ),

    positiveCanvas: document.getElementById(
        "positiveChart"
    ),
    negativeCanvas: document.getElementById(
        "negativeChart"
    ),
    sentimentClassCanvas: document.getElementById(
        "sentimentClassChart"
    )
};

let positiveChart;
let negativeChart;
let sentimentClassChart;

let pollingInterval = null;
let trackingActive = false;

let firstTrackedValue = "";
let secondTrackedValue = "";

const firstColor = "#4f46e5";
const secondColor = "#0891b2";

document.addEventListener("DOMContentLoaded", () => {
    initializeCharts();
    bindEvents();
    updateCharacterCounts();
    checkModelStatus();
});

function bindEvents() {
    elements.form.addEventListener(
        "submit",
        startComparison
    );

    elements.stopButton.addEventListener(
        "click",
        stopComparison
    );

    elements.firstInput.addEventListener(
        "input",
        updateCharacterCounts
    );

    elements.secondInput.addEventListener(
        "input",
        updateCharacterCounts
    );
}

function initializeCharts() {
    positiveChart = createProbabilityChart(
        elements.positiveCanvas,
        "Positive probability"
    );

    negativeChart = createProbabilityChart(
        elements.negativeCanvas,
        "Negative probability"
    );

    sentimentClassChart = new Chart(
        elements.sentimentClassCanvas,
        {
            type: "line",

            data: {
                labels: [],
                datasets: []
            },

            options: {
                responsive: true,
                maintainAspectRatio: false,

                interaction: {
                    mode: "index",
                    intersect: false
                },

                scales: {
                    x: {
                        title: {
                            display: true,
                            text: "Model version / event time"
                        }
                    },

                    y: {
                        min: -1.2,
                        max: 1.2,

                        ticks: {
                            stepSize: 1,

                            callback(value) {
                                if (value === 1) {
                                    return "Positive";
                                }

                                if (value === -1) {
                                    return "Negative";
                                }

                                return "";
                            }
                        }
                    }
                },

                plugins: {
                    legend: {
                        position: "bottom",
                        labels: {
                            usePointStyle: true,
                            padding: 20
                        }
                    },

                    tooltip: {
                        callbacks: {
                            label(context) {
                                const sentiment =
                                    context.parsed.y === 1
                                        ? "Positive"
                                        : "Negative";

                                return (
                                    `${context.dataset.label}: ` +
                                    sentiment
                                );
                            }
                        }
                    }
                }
            }
        }
    );
}

function createProbabilityChart(canvas, yAxisTitle) {
    return new Chart(canvas, {
        type: "line",

        data: {
            labels: [],
            datasets: []
        },

        options: {
            responsive: true,
            maintainAspectRatio: false,

            interaction: {
                mode: "index",
                intersect: false
            },

            scales: {
                x: {
                    title: {
                        display: true,
                        text: "Model version / event time"
                    }
                },

                y: {
                    min: 0,
                    max: 1,

                    title: {
                        display: true,
                        text: yAxisTitle
                    },

                    ticks: {
                        callback(value) {
                            return `${Math.round(value * 100)}%`;
                        }
                    }
                }
            },

            plugins: {
                legend: {
                    position: "bottom",

                    labels: {
                        usePointStyle: true,
                        padding: 20
                    }
                },

                tooltip: {
                    callbacks: {
                        label(context) {
                            return (
                                `${context.dataset.label}: ` +
                                `${formatPercentage(
                                    context.parsed.y
                                )}`
                            );
                        }
                    }
                }
            }
        }
    });
}

async function checkModelStatus() {
    try {
        const response = await fetch(
            `${API_BASE_URL}/model/status`
        );

        const data = await readJsonResponse(response);

        if (!response.ok) {
            throw new Error(
                data.detail || "Unable to check model status."
            );
        }

        if (data.available) {
            elements.modelStatus.className =
                "status-badge status-online";

            elements.modelStatusText.textContent =
                `Model ready · ${data.total_versions} version` +
                `${data.total_versions === 1 ? "" : "s"}`;
        } else {
            elements.modelStatus.className =
                "status-badge status-offline";

            elements.modelStatusText.textContent =
                data.message || "No model available";
        }
    } catch (error) {
        elements.modelStatus.className =
            "status-badge status-offline";

        elements.modelStatusText.textContent =
            "Backend unavailable";

        console.error(error);
    }
}

async function startComparison(event) {
    event.preventDefault();

    const firstValue = elements.firstInput.value.trim();
    const secondValue = elements.secondInput.value.trim();

    if (!firstValue || !secondValue) {
        showMessage(
            "Enter both keywords or sentences.",
            "error"
        );
        return;
    }

    if (firstValue === secondValue) {
        showMessage(
            "The two comparison values must be different.",
            "error"
        );
        return;
    }

    firstTrackedValue = firstValue;
    secondTrackedValue = secondValue;

    setLoading(true);
    hideMessage();

    try {
        const response = await fetch(
            `${API_BASE_URL}/watch`,
            {
                method: "POST",

                headers: {
                    "Content-Type": "application/json"
                },

                body: JSON.stringify({
                    keywords: [
                        firstTrackedValue,
                        secondTrackedValue
                    ]
                })
            }
        );

        const result = await readJsonResponse(response);

        if (!response.ok) {
            throw new Error(
                result.detail ||
                "Unable to start sentiment comparison."
            );
        }

        trackingActive = true;

        elements.comparisonSummary.hidden = false;
        elements.liveStatus.hidden = false;
        elements.stopButton.hidden = false;

        elements.firstResultName.textContent =
            firstTrackedValue;

        elements.secondResultName.textContent =
            secondTrackedValue;

        elements.firstTableHeader.textContent =
            shortenText(firstTrackedValue, 24);

        elements.secondTableHeader.textContent =
            shortenText(secondTrackedValue, 24);

        showMessage(
            `Comparing both inputs across ` +
            `${result.backfill_versions || 0} model versions.`,
            "success"
        );

        await loadSeries();

        if (pollingInterval) {
            window.clearInterval(pollingInterval);
        }

        pollingInterval = window.setInterval(
            loadSeries,
            REFRESH_INTERVAL
        );
    } catch (error) {
        showMessage(error.message, "error");
        console.error(error);
    } finally {
        setLoading(false);
    }
}

function stopComparison() {
    trackingActive = false;

    if (pollingInterval) {
        window.clearInterval(pollingInterval);
        pollingInterval = null;
    }

    elements.liveStatus.hidden = true;
    elements.stopButton.hidden = true;

    showMessage(
        "Live frontend updates have been stopped.",
        "info"
    );
}

async function loadSeries() {
    if (!trackingActive) {
        return;
    }

    try {
        const response = await fetch(
            `${API_BASE_URL}/series`,
            {
                cache: "no-store"
            }
        );

        const seriesData = await readJsonResponse(response);

        if (!response.ok) {
            throw new Error(
                seriesData.detail ||
                "Unable to load sentiment series."
            );
        }

        renderComparison(seriesData);
    } catch (error) {
        showMessage(
            `Unable to refresh comparison: ${error.message}`,
            "error"
        );

        console.error(error);
    }
}

function renderComparison(seriesData) {
    const firstSeries = Array.isArray(
        seriesData[firstTrackedValue]
    )
        ? seriesData[firstTrackedValue]
        : [];

    const secondSeries = Array.isArray(
        seriesData[secondTrackedValue]
    )
        ? seriesData[secondTrackedValue]
        : [];

    renderProbabilityCharts(
        firstSeries,
        secondSeries
    );

    renderClassChart(
        firstSeries,
        secondSeries
    );

    renderLatestSummary(
        firstSeries,
        secondSeries
    );

    renderHistoryTable(
        firstSeries,
        secondSeries
    );
}

function renderProbabilityCharts(
    firstSeries,
    secondSeries
) {
    const eventTimes = getCombinedEventTimes(
        firstSeries,
        secondSeries
    );

    const labels = eventTimes.map(formatEventTime);

    const firstMap = createPointMap(firstSeries);
    const secondMap = createPointMap(secondSeries);

    positiveChart.data.labels = labels;
    positiveChart.data.datasets = [
        createDataset(
            shortenText(firstTrackedValue, 35),
            eventTimes.map((eventTime) => {
                const point = firstMap.get(
                    String(eventTime)
                );

                return point
                    ? normalizeProbabilities(
                        point.proba
                    ).positive
                    : null;
            }),
            firstColor
        ),

        createDataset(
            shortenText(secondTrackedValue, 35),
            eventTimes.map((eventTime) => {
                const point = secondMap.get(
                    String(eventTime)
                );

                return point
                    ? normalizeProbabilities(
                        point.proba
                    ).positive
                    : null;
            }),
            secondColor
        )
    ];

    positiveChart.update();

    negativeChart.data.labels = labels;
    negativeChart.data.datasets = [
        createDataset(
            shortenText(firstTrackedValue, 35),
            eventTimes.map((eventTime) => {
                const point = firstMap.get(
                    String(eventTime)
                );

                return point
                    ? normalizeProbabilities(
                        point.proba
                    ).negative
                    : null;
            }),
            firstColor
        ),

        createDataset(
            shortenText(secondTrackedValue, 35),
            eventTimes.map((eventTime) => {
                const point = secondMap.get(
                    String(eventTime)
                );

                return point
                    ? normalizeProbabilities(
                        point.proba
                    ).negative
                    : null;
            }),
            secondColor
        )
    ];

    negativeChart.update();
}

function renderClassChart(firstSeries, secondSeries) {
    const eventTimes = getCombinedEventTimes(
        firstSeries,
        secondSeries
    );

    const firstMap = createPointMap(firstSeries);
    const secondMap = createPointMap(secondSeries);

    sentimentClassChart.data.labels =
        eventTimes.map(formatEventTime);

    sentimentClassChart.data.datasets = [
        createDataset(
            shortenText(firstTrackedValue, 35),
            eventTimes.map((eventTime) => {
                const point = firstMap.get(
                    String(eventTime)
                );

                return point
                    ? sentimentToScore(point.label)
                    : null;
            }),
            firstColor
        ),

        createDataset(
            shortenText(secondTrackedValue, 35),
            eventTimes.map((eventTime) => {
                const point = secondMap.get(
                    String(eventTime)
                );

                return point
                    ? sentimentToScore(point.label)
                    : null;
            }),
            secondColor
        )
    ];

    sentimentClassChart.update();
}

function renderLatestSummary(
    firstSeries,
    secondSeries
) {
    const firstLatest = getLatestPoint(firstSeries);
    const secondLatest = getLatestPoint(secondSeries);

    if (firstLatest) {
        const probabilities = normalizeProbabilities(
            firstLatest.proba
        );

        const sentiment = normalizeLabel(
            firstLatest.label
        );

        elements.firstSentiment.textContent =
            capitalize(sentiment);

        elements.firstSentiment.className =
            `sentiment-badge sentiment-${sentiment}`;

        elements.firstPositive.textContent =
            formatPercentage(probabilities.positive);

        elements.firstNegative.textContent =
            formatPercentage(probabilities.negative);
    }

    if (secondLatest) {
        const probabilities = normalizeProbabilities(
            secondLatest.proba
        );

        const sentiment = normalizeLabel(
            secondLatest.label
        );

        elements.secondSentiment.textContent =
            capitalize(sentiment);

        elements.secondSentiment.className =
            `sentiment-badge sentiment-${sentiment}`;

        elements.secondPositive.textContent =
            formatPercentage(probabilities.positive);

        elements.secondNegative.textContent =
            formatPercentage(probabilities.negative);
    }

    if (!firstLatest || !secondLatest) {
        elements.comparisonTitle.textContent =
            "Waiting for both results";

        elements.comparisonWinner.textContent =
            "Both inputs need an inference result before they can be compared.";

        return;
    }

    const firstProbabilities = normalizeProbabilities(
        firstLatest.proba
    );

    const secondProbabilities = normalizeProbabilities(
        secondLatest.proba
    );

    const difference = Math.abs(
        firstProbabilities.positive -
        secondProbabilities.positive
    );

    elements.positiveDifference.textContent =
        formatPercentage(difference);

    elements.differenceBar.style.width =
        `${Math.min(difference * 100, 100)}%`;

    if (
        firstProbabilities.positive >
        secondProbabilities.positive
    ) {
        elements.comparisonTitle.textContent =
            `${shortenText(firstTrackedValue, 30)} is more positive`;

        elements.comparisonWinner.textContent =
            `${firstTrackedValue} has a higher positive probability ` +
            `by ${formatPercentage(difference)}.`;
    } else if (
        secondProbabilities.positive >
        firstProbabilities.positive
    ) {
        elements.comparisonTitle.textContent =
            `${shortenText(secondTrackedValue, 30)} is more positive`;

        elements.comparisonWinner.textContent =
            `${secondTrackedValue} has a higher positive probability ` +
            `by ${formatPercentage(difference)}.`;
    } else {
        elements.comparisonTitle.textContent =
            "Equal positive probability";

        elements.comparisonWinner.textContent =
            "Both values currently have the same positive probability.";
    }
}

function renderHistoryTable(
    firstSeries,
    secondSeries
) {
    const eventTimes = getCombinedEventTimes(
        firstSeries,
        secondSeries
    ).reverse();

    if (eventTimes.length === 0) {
        elements.historyTableBody.innerHTML = `
            <tr>
                <td colspan="8" class="empty-table-cell">
                    Waiting for inference results...
                </td>
            </tr>
        `;

        return;
    }

    const firstMap = createPointMap(firstSeries);
    const secondMap = createPointMap(secondSeries);

    elements.historyTableBody.innerHTML = eventTimes
        .map((eventTime) => {
            const firstPoint = firstMap.get(
                String(eventTime)
            );

            const secondPoint = secondMap.get(
                String(eventTime)
            );

            const firstProbabilities = firstPoint
                ? normalizeProbabilities(firstPoint.proba)
                : null;

            const secondProbabilities = secondPoint
                ? normalizeProbabilities(secondPoint.proba)
                : null;

            const firstLabel = firstPoint
                ? normalizeLabel(firstPoint.label)
                : null;

            const secondLabel = secondPoint
                ? normalizeLabel(secondPoint.label)
                : null;

            const winner = getVersionWinner(
                firstProbabilities,
                secondProbabilities
            );

            return `
                <tr>
                    <td>
                        ${escapeHtml(
                            formatEventTime(eventTime)
                        )}
                    </td>

                    <td>
                        ${renderSentimentBadge(firstLabel)}
                    </td>

                    <td>
                        ${
                            firstProbabilities
                                ? formatPercentage(
                                    firstProbabilities.positive
                                )
                                : "—"
                        }
                    </td>

                    <td>
                        ${
                            firstProbabilities
                                ? formatPercentage(
                                    firstProbabilities.negative
                                )
                                : "—"
                        }
                    </td>

                    <td>
                        ${renderSentimentBadge(secondLabel)}
                    </td>

                    <td>
                        ${
                            secondProbabilities
                                ? formatPercentage(
                                    secondProbabilities.positive
                                )
                                : "—"
                        }
                    </td>

                    <td>
                        ${
                            secondProbabilities
                                ? formatPercentage(
                                    secondProbabilities.negative
                                )
                                : "—"
                        }
                    </td>

                    <td>
                        ${escapeHtml(winner)}
                    </td>
                </tr>
            `;
        })
        .join("");
}

function getVersionWinner(
    firstProbabilities,
    secondProbabilities
) {
    if (!firstProbabilities || !secondProbabilities) {
        return "Waiting";
    }

    if (
        firstProbabilities.positive >
        secondProbabilities.positive
    ) {
        return shortenText(firstTrackedValue, 25);
    }

    if (
        secondProbabilities.positive >
        firstProbabilities.positive
    ) {
        return shortenText(secondTrackedValue, 25);
    }

    return "Equal";
}

function getCombinedEventTimes(
    firstSeries,
    secondSeries
) {
    const eventTimes = new Set();

    for (const point of [
        ...firstSeries,
        ...secondSeries
    ]) {
        if (
            point.event_time !== undefined &&
            point.event_time !== null
        ) {
            eventTimes.add(point.event_time);
        }
    }

    return [...eventTimes].sort(compareEventTimes);
}

function createPointMap(series) {
    return new Map(
        series.map((point) => [
            String(point.event_time),
            point
        ])
    );
}

function createDataset(label, data, color) {
    return {
        label,
        data,
        borderColor: color,
        backgroundColor: color,
        pointBackgroundColor: color,
        borderWidth: 3,
        pointRadius: 4,
        pointHoverRadius: 6,
        tension: 0.25,
        spanGaps: true
    };
}

function getLatestPoint(series) {
    if (!Array.isArray(series) || series.length === 0) {
        return null;
    }

    return [...series].sort((a, b) => {
        return compareEventTimes(
            b.event_time,
            a.event_time
        );
    })[0];
}

function normalizeProbabilities(proba) {
    const source =
        proba && typeof proba === "object"
            ? proba
            : {};

    const normalized = {};

    for (const [key, value] of Object.entries(source)) {
        normalized[String(key).toLowerCase()] =
            Number(value);
    }

    let positive = getFirstNumber(
        normalized,
        [
            "positive",
            "pos",
            "1",
            "label_1"
        ]
    );

    let negative = getFirstNumber(
        normalized,
        [
            "negative",
            "neg",
            "0",
            "-1",
            "label_0"
        ]
    );

    /*
     * If the backend only returns one class probability,
     * calculate the other probability.
     */
    if (positive === null && negative !== null) {
        positive = 1 - negative;
    }

    if (negative === null && positive !== null) {
        negative = 1 - positive;
    }

    positive = clampProbability(positive ?? 0);
    negative = clampProbability(negative ?? 0);

    const total = positive + negative;

    /*
     * Normalize both values when they do not add up to 1.
     */
    if (total > 0 && Math.abs(total - 1) > 0.001) {
        positive /= total;
        negative /= total;
    }

    return {
        positive,
        negative
    };
}

function getFirstNumber(source, possibleKeys) {
    for (const key of possibleKeys) {
        if (source[key] === undefined) {
            continue;
        }

        const value = Number(source[key]);

        if (!Number.isFinite(value)) {
            continue;
        }

        return value > 1
            ? value / 100
            : value;
    }

    return null;
}

function normalizeLabel(label) {
    const value = String(label || "")
        .trim()
        .toLowerCase();

    if (
        value.includes("positive") ||
        value === "pos" ||
        value === "1" ||
        value === "label_1"
    ) {
        return "positive";
    }

    return "negative";
}

function sentimentToScore(label) {
    return normalizeLabel(label) === "positive"
        ? 1
        : -1;
}

function renderSentimentBadge(label) {
    if (!label) {
        return "—";
    }

    return `
        <span class="
            table-sentiment
            sentiment-${label}
        ">
            ${capitalize(label)}
        </span>
    `;
}

function updateCharacterCounts() {
    elements.firstCharacterCount.textContent =
        elements.firstInput.value.length;

    elements.secondCharacterCount.textContent =
        elements.secondInput.value.length;
}

function setLoading(loading) {
    elements.compareButton.disabled = loading;

    elements.buttonLoader.hidden = !loading;

    elements.buttonText.textContent = loading
        ? "Starting comparison..."
        : "Compare over time";
}

function showMessage(message, type) {
    elements.messageBox.hidden = false;
    elements.messageBox.textContent = message;
    elements.messageBox.className =
        `message-box message-${type}`;
}

function hideMessage() {
    elements.messageBox.hidden = true;
    elements.messageBox.textContent = "";
}

async function readJsonResponse(response) {
    const text = await response.text();

    if (!text) {
        return {};
    }

    try {
        return JSON.parse(text);
    } catch {
        return {
            detail: text
        };
    }
}

function compareEventTimes(firstValue, secondValue) {
    const firstNumber = Number(firstValue);
    const secondNumber = Number(secondValue);

    if (
        Number.isFinite(firstNumber) &&
        Number.isFinite(secondNumber)
    ) {
        return firstNumber - secondNumber;
    }

    return String(firstValue).localeCompare(
        String(secondValue),
        undefined,
        {
            numeric: true
        }
    );
}

function formatEventTime(eventTime) {
    if (
        eventTime === undefined ||
        eventTime === null
    ) {
        return "Unknown";
    }

    const numericValue = Number(eventTime);

    if (!Number.isFinite(numericValue)) {
        return String(eventTime);
    }

    if (numericValue > 100000000000) {
        return new Date(
            numericValue
        ).toLocaleString();
    }

    if (numericValue > 1000000000) {
        return new Date(
            numericValue * 1000
        ).toLocaleString();
    }

    return String(eventTime);
}

function clampProbability(value) {
    const number = Number(value);

    if (!Number.isFinite(number)) {
        return 0;
    }

    return Math.min(1, Math.max(0, number));
}

function formatPercentage(value) {
    return `${(
        clampProbability(value) * 100
    ).toFixed(1)}%`;
}

function shortenText(value, maximumLength) {
    const text = String(value || "");

    if (text.length <= maximumLength) {
        return text;
    }

    return `${text.slice(0, maximumLength - 1)}…`;
}

function capitalize(value) {
    const text = String(value || "");

    return (
        text.charAt(0).toUpperCase() +
        text.slice(1)
    );
}

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

