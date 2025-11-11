const initState = () => {
  return {
    data: [],
    layout: {
      yaxis: {
        scaleanchor: "x",
        visible: false,
      },
      xaxis: {
        visible: false,
      },
      plot_bgcolor: "rgba(0,0,0,0)",
      width: 400,
      height: 450,
      margin: {
        b: 0,
        l: 0,
        pad: 0,
        r: 0,
        t: 0,
      },
      annotations: [
        {
          text: "loading...",
          xref: "paper",
          yref: "paper",
          showarrow: false,
          font: {
            size: 28,
          },
        },
      ],
    },
  };
};

export { initState };
