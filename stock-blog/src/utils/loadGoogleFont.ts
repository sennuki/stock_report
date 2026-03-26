async function loadGoogleFont(
  font: string,
  text: string,
  weight: number
): Promise<ArrayBuffer> {
  const API = `https://fonts.googleapis.com/css2?family=${font}:wght@${weight}&text=${encodeURIComponent(text)}`;

  const maxAttempts = 3;
  let attempts = 0;

  while (attempts <= maxAttempts) {
    try {
      const cssResponse = await fetch(API, {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; de-at) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1",
        },
      });

      if (!cssResponse.ok) {
        if (cssResponse.status === 502 && attempts < maxAttempts) {
          throw new Error("502");
        }
        throw new Error(`CSS fetch failed with status: ${cssResponse.status}`);
      }

      const css = await cssResponse.text();
      const resource = css.match(
        /src: url\((.+?)\) format\('(opentype|truetype)'\)/
      );

      if (!resource) throw new Error("Failed to parse font URL from CSS");

      const res = await fetch(resource[1]);

      if (!res.ok) {
        if (res.status === 502 && attempts < maxAttempts) {
          throw new Error("502");
        }
        throw new Error(`Font fetch failed with status: ${res.status}`);
      }

      return await res.arrayBuffer();
    } catch (error: any) {
      if (error.message === "502" && attempts < maxAttempts) {
        attempts++;
        console.warn(
          `Failed to download dynamic font (502). Retrying attempt ${attempts}...`
        );
        await new Promise(resolve => setTimeout(resolve, 1000 * attempts));
        continue;
      }
      throw error;
    }
  }

  throw new Error("Failed to download dynamic font after maximum attempts");
}

async function loadGoogleFonts(
  text: string
): Promise<
  Array<{ name: string; data: ArrayBuffer; weight: number; style: string }>
> {
  const fontsConfig = [
    {
      name: "IBM Plex Mono",
      font: "IBM+Plex+Mono",
      weight: 400,
      style: "normal",
    },
    {
      name: "IBM Plex Mono",
      font: "IBM+Plex+Mono",
      weight: 700,
      style: "bold",
    },
  ];

  const fonts = await Promise.all(
    fontsConfig.map(async ({ name, font, weight, style }) => {
      const data = await loadGoogleFont(font, text, weight);
      return { name, data, weight, style };
    })
  );

  return fonts;
}

export default loadGoogleFonts;
