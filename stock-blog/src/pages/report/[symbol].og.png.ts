import satori from "satori";
import stocks from "@/data/stocks.json";

const height = 630;
const width = 1200;

async function generateStockOG(symbol: string, price?: number, change?: number) {
  const priceDisplay = price ? `$${price.toFixed(2)}` : "N/A";
  const changeColor = change && change >= 0 ? "#10b981" : "#ef4444";
  const changeText = change ? `${change >= 0 ? "+" : ""}${change.toFixed(2)}%` : "";

  const svg = await satori(
    {
      type: "div",
      props: {
        style: {
          display: "flex",
          width: "100%",
          height: "100%",
          backgroundImage: "linear-gradient(135deg, #1e293b 0%, #0f172a 100%)",
          padding: "60px",
          fontFamily: "system-ui, -apple-system, sans-serif",
        },
        children: [
          {
            type: "div",
            props: {
              style: {
                display: "flex",
                flexDirection: "column",
                flex: 1,
                justifyContent: "center",
                gap: "30px",
              },
              children: [
                {
                  type: "div",
                  props: {
                    style: {
                      fontSize: "96px",
                      fontWeight: "bold",
                      color: "#ffffff",
                      letterSpacing: "-2px",
                    },
                    children: symbol,
                  },
                },
                {
                  type: "div",
                  props: {
                    style: {
                      display: "flex",
                      gap: "40px",
                      alignItems: "baseline",
                    },
                    children: [
                      {
                        type: "div",
                        props: {
                          style: {
                            fontSize: "64px",
                            fontWeight: "bold",
                            color: "#ffffff",
                          },
                          children: priceDisplay,
                        },
                      },
                      {
                        type: "div",
                        props: {
                          style: {
                            fontSize: "36px",
                            fontWeight: "600",
                            color: changeColor,
                          },
                          children: changeText,
                        },
                      },
                    ],
                  },
                },
              ],
            },
          },
          {
            type: "div",
            props: {
              style: {
                display: "flex",
                flexDirection: "column",
                justifyContent: "flex-end",
                gap: "15px",
                textAlign: "right",
              },
              children: [
                {
                  type: "div",
                  props: {
                    style: {
                      fontSize: "24px",
                      color: "#94a3b8",
                    },
                    children: "Stock Report",
                  },
                },
                {
                  type: "div",
                  props: {
                    style: {
                      fontSize: "16px",
                      color: "#64748b",
                    },
                    children: "stockreport.jp",
                  },
                },
              ],
            },
          },
        ],
      },
    },
    {
      width,
      height,
      fonts: [
        {
          name: "system-ui",
          data: await fetch(
            "https://fonts.gstatic.com/s/nunitosans/v14/pe0eMIySN5p3EwC9xvjQEnMwYzlM-vdpDg.0.woff2"
          ).then(r => r.arrayBuffer()),
          weight: 700,
          style: "normal",
        },
      ],
    }
  );

  const resvg = (await import("@resvg/resvg-js")).default;
  const pngData = resvg.render(Buffer.from(svg)).asPng();

  return pngData;
}

export async function getStaticPaths() {
  return (stocks as any[]).map((stock: any) => ({
    params: { symbol: stock.Symbol_YF },
  }));
}

export async function GET({ params }) {
  try {
    const { symbol } = params;
    const pngData = await generateStockOG(symbol);

    return new Response(pngData, {
      headers: {
        "Content-Type": "image/png",
        "Cache-Control": "public, max-age=86400, immutable",
      },
    });
  } catch (error) {
    console.error("Error generating stock OG image:", error);
    return new Response("Failed to generate OG image", { status: 500 });
  }
}
