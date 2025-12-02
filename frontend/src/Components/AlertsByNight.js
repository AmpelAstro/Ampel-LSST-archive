import { useState, useEffect } from "react";
import axios from "axios";
import { useParams } from "react-router-dom";
import { JSONParse } from "json-with-bigint";

import "react-tabulator/lib/styles.css"; // required styles
import "react-tabulator/css/tabulator_bootstrap5.min.css"; // theme
import { ReactTabulator } from "react-tabulator";

//custom max min header filter
const minMaxFilterEditor = function (
  cell,
  onRendered,
  success,
  cancel,
  editorParams
) {
  var end;

  var container = document.createElement("span");

  //create and style inputs
  var start = document.createElement("input");
  start.setAttribute("type", "number");
  start.setAttribute("placeholder", "Min");
  start.setAttribute("min", 0);
  start.setAttribute("max", 100);
  start.style.padding = "4px";
  start.style.width = "50%";
  start.style.boxSizing = "border-box";

  start.value = cell.getValue();

  function buildValues() {
    success({
      start: start.value,
      end: end.value,
    });
  }

  function keypress(e) {
    if (e.keyCode === 13) {
      buildValues();
    }

    if (e.keyCode === 27) {
      cancel();
    }
  }

  end = start.cloneNode();
  end.setAttribute("placeholder", "Max");

  start.addEventListener("change", buildValues);
  start.addEventListener("blur", buildValues);
  start.addEventListener("keydown", keypress);

  end.addEventListener("change", buildValues);
  end.addEventListener("blur", buildValues);
  end.addEventListener("keydown", keypress);

  container.appendChild(start);
  container.appendChild(end);

  return container;
};

//custom max min filter function
const minMaxFilterFunction = function (
  headerValue,
  rowValue,
  rowData,
  filterParams
) {
  //headerValue - the value of the header filter element
  //rowValue - the value of the column in this row
  //rowData - the data for the row being filtered
  //filterParams - params object passed to the headerFilterFuncParams property

  if (rowValue) {
    if (headerValue.start !== "") {
      if (headerValue.end !== "") {
        return rowValue >= headerValue.start && rowValue <= headerValue.end;
      } else {
        return rowValue >= headerValue.start;
      }
    } else {
      if (headerValue.end !== "") {
        return rowValue <= headerValue.end;
      }
    }
  }

  // only return true on null if filter is empty
  return (headerValue.start === "") && (headerValue.end === "");
};

const AlertsByNight = () => {
  let params = useParams();

  // Keep a local id state, synced with the URL param
  const [idState, setIdState] = useState(params.nightId);
  const [content, setContent] = useState([]);

  // Sync local state if the URL param changes (e.g., via navigate or manual URL entry)
  useEffect(() => {
    if (params.nightId && params.nightId !== idState) {
      setContent([]);
      setIdState(params.nightId);
    }
  }, [params.nightId, idState]);
  // Fetch alert details whenever the current id changes
  useEffect(() => {
    if (!idState) return;

    const fetchData = async () => {
      try {
        // parse response with JSONParse to handle BigInt
        const response = await axios.post(
          `${process.env.REACT_APP_API_BASE}/display/alerts/query`,
          {
            include: [
              "diaSourceId",
              "diaSource.reliability",
              "diaSource.psfFlux",
              "diaObject.nDiaSources",
              "diaSource.snr",
            ],
            condition: `diaSource.visit >= ${idState}00000 and diaSource.visit < ${
              parseInt(idState) + 1
            }00000`,
          },
          { transformResponse: [(data) => JSONParse(data)] }
        );
        setContent(response.data);
      } catch (error) {
        //   setError(error);
      } finally {
        //   setLoading(false);
      }
    };

    fetchData();
  }, [idState]);

  const columns = [
    {
      title: "alert",
      field: "diaSourceId",
      width: 150,
      headerSort: false,
      formatter: "link",
      formatterParams: {
        labelField: "diaSourceId",
        urlPrefix: `${process.env.PUBLIC_URL}/alert/`,
        target: "_blank",
      },
    },
    {
      title: "reliability",
      field: "reliability",
      hozAlign: "left",
      headerFilter: "progress",
      formatter: "progress",
      formatterParams: { min: 0, max: 1 },
    },
    {
      title: "psfFlux",
      field: "psfFlux",
      hozAlign: "left",
      formatter: "money",
      formatterParams: { precision: 2, thousand: "_" },
    },
    {
      title: "snr",
      field: "snr",
      formatter: "money",
      formatterParams: { precision: 2, thousand: "_" },
    },
    {
      title: "detections",
      field: "nDiaSources",
      headerFilter: minMaxFilterEditor,
      headerFilterFunc: minMaxFilterFunction,
      headerFilterLiveFilter: false,
    },
  ];

  console.log("hola", params);

  // NB: fix height for performance reasons; without height constraint all content is rendered immediately
  return (
    <div>
      <div className="mx-1 mt-2">
        <h5 className="mb-3">Night {idState}</h5>
      </div>
      <ReactTabulator columns={columns} data={content} height={400} />
    </div>
  );
};

export default AlertsByNight;
