const baseUrl = "https://api.metalpriceapi.com/v1/";
const API_KEY = "f7310b277b44abb93d91fc3e62481199"; // replace with your real API key
function formatUSD(amount) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
  }).format(amount);
}

function formatINR(amount) {
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
  }).format(amount);
}

const formatDate = (date) =>
  `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(
    2,
    "0"
  )}-${String(date.getDate()).padStart(2, "0")}`;

const formatDateForChart = (dateStr) => {
  const date = new Date(dateStr);
  return `${String(date.getMonth() + 1).padStart(2, "0")}/${String(
    date.getDate()
  ).padStart(2, "0")}`;
};

async function fetchLiveGoldDataForTable() {
  const [goldRes, inrRes] = await Promise.all([
    fetch(`${baseUrl}carat?api_key=${API_KEY}&base=USD`),
    fetch(`${baseUrl}latest?api_key=${API_KEY}&base=USD&currencies=INR`),
  ]);

  const goldData = await goldRes.json();
  const inrRate = (await inrRes.json()).rates.INR;
  const carats = ["24k", "22k", "18k"];

  const units = [
    { label: "1 Ounce", multiplier: 31.1 },
    { label: "10 gram", multiplier: 10 },
    { label: "1 Tola (11.66 Grams)", multiplier: 11.66 },
    { label: "1 gram", multiplier: 1 },
  ];

  const html = carats
    .map((karat) => {
      const base = goldData.data[karat] * 5;
      return `
        <table>
          <tr><th colspan="3">${karat}t Gold Price</th></tr>
          ${units
            .map(
              ({ label, multiplier }) => `
            <tr>
              <td>${label}</td>
              <td>${formatUSD(base * multiplier)}</td>
              <td>${formatINR(base * multiplier * inrRate)}</td>
            </tr>
          `
            )
            .join("")}
        </table>`;
    })
    .join("");

  document.querySelector(".live-price").innerHTML = html;
}

async function fetchLiveGoldDataForChart() {
  const today = new Date();
  const tenDaysAgo = new Date();
  tenDaysAgo.setDate(today.getDate() - 10);

  const [historyRes, latestRes] = await Promise.all([
    fetch(
      `${baseUrl}timeframe?api_key=${API_KEY}&base=USD&currencies=XAU&start_date=${formatDate(
        tenDaysAgo
      )}&end_date=${formatDate(today)}`
    ),
    fetch(`${baseUrl}latest?api_key=${API_KEY}&base=USD&currencies=XAU`),
  ]);

  const historyData = await historyRes.json();
  const latestPrice = (await latestRes.json()).rates.USDXAU;

  const xValues = [];
  const yValues = [];

  for (const date in historyData.rates) {
    xValues.push(formatDateForChart(date));
    yValues.push(historyData.rates[date].USDXAU);
  }

  xValues.push(formatDateForChart(today.toISOString().split("T")[0]));
  yValues.push(latestPrice);

  const ctx = document.getElementById("myChart").getContext("2d");

  new Chart(ctx, {
    type: "line",
    data: {
      labels: xValues,
      datasets: [
        {
          fill: true,
          lineTension: 0,
          backgroundColor: "rgba(207,169,107,0.2)",
          borderColor: "rgba(207,169,107,1)",
          borderWidth: 2,
          pointRadius: 2,
          pointBackgroundColor: function (ctx) {
            return ctx.dataIndex === yValues.length - 1
              ? "rgba(255, 0, 0, 1)"
              : "rgba(207,169,107,1)";
          },
          data: yValues,
        },
      ],
    },
    options: {
      plugins: {
        datalabels: {
          display: false,
        },
      },
      animation: false,
      title: {
        display: true,
        text: "Live Gold Price (USD/oz)",
        fontSize: 16,
      },
      legend: { display: false },
      scales: {
        yAxes: [
          {
            ticks: {
              beginAtZero: false,
              suggestedMin: 3000,
              suggestedMax: 3500,
            },
          },
        ],
      },
    },
  });
}

async function fetch2025HighAndRenderChart() {
  const barCtx = document.getElementById("tenYearChart").getContext("2d");

  const yearLabels = [
    "2015",
    "2016",
    "2017",
    "2018",
    "2019",
    "2020",
    "2021",
    "2022",
    "2023",
    "2024",
    "2025",
  ];

  const yearPrices = [
    1298,
    1373,
    1351,
    1360,
    1543,
    2058,
    1954,
    2043,
    2115,
    2786,
    3500, // Verified 2025 high
  ];

  new Chart(barCtx, {
    type: "bar",
    data: {
      labels: yearLabels,
      datasets: [
        {
          label: "Gold Price (USD/oz)",
          data: yearPrices,
          backgroundColor: "rgba(207,169,107,0.8)",
          borderColor: "rgba(207,169,107,1)",
          borderWidth: 1,
        },
      ],
    },
    options: {
      plugins: {
        datalabels: {
          color: "#000",
          anchor: "end",
          align: "end",
          font: {
            weight: "bold",
          },
          formatter: (value) => `$${value.toFixed(2)}`,
        },
      },
      title: {
        display: true,
        text: "Gold Price Increase Over the Last 10 Years",
        fontSize: 16,
      },
      legend: {
        display: false,
      },
      scales: {
        yAxes: [
          {
            ticks: {
              beginAtZero: false,
              suggestedMin: 1000,
              suggestedMax: 4000,
              callback: (value) =>
                `$${value.toLocaleString(undefined, {
                  minimumFractionDigits: 2,
                })}`,
            },
          },
        ],
      },
    },
    plugins: [ChartDataLabels],
  });
}

// Initialize all
fetchLiveGoldDataForTable();
fetchLiveGoldDataForChart();
fetch2025HighAndRenderChart();
