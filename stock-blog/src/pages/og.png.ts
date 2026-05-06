import satori from "satori";
import { SITE } from "@/config";

const height = 630;
const width = 1200;

async function generateOG() {
  const svg = await satori(
    {
      type: "div",
      props: {
        children: [
          {
            type: "div",
            props: {
              style: {
                display: "flex",
                flexDirection: "column",
                width: "100%",
                height: "100%",
                padding: "60px",
                backgroundImage:
                  "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
                alignItems: "center",
                justifyContent: "center",
                textAlign: "center",
                fontFamily: "system-ui, -apple-system, sans-serif",
              },
              children: [
                {
                  type: "div",
                  props: {
                    style: {
                      fontSize: "72px",
                      fontWeight: "bold",
                      color: "white",
                      marginBottom: "20px",
                    },
                    children: SITE.title,
                  },
                },
                {
                  type: "div",
                  props: {
                    style: {
                      fontSize: "32px",
                      color: "rgba(255, 255, 255, 0.9)",
                      marginBottom: "40px",
                    },
                    children: SITE.desc,
                  },
                },
                {
                  type: "div",
                  props: {
                    style: {
                      fontSize: "18px",
                      color: "rgba(255, 255, 255, 0.7)",
                    },
                    children: SITE.website,
                  },
                },
              ],
            },
          },
        ],
        style: {
          display: "flex",
          width: "100%",
          height: "100%",
        },
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

  const { Resvg } = await import("@resvg/resvg-js");
  const resvg = new Resvg(svg);
  const pngBuffer = resvg.render().asPng();

  return new Uint8Array(pngBuffer);
}

export async function GET() {
  try {
    const pngData = await generateOG();
    return new Response(pngData, {
      headers: {
        "Content-Type": "image/png",
        "Cache-Control": "public, max-age=3600, immutable",
      },
    });
  } catch (error) {
    console.error("Error generating OG image:", error);
    return new Response("Failed to generate OG image", { status: 500 });
  }
}
