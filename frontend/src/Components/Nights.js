import { useState, useEffect } from "react";
import axios from "axios";

import "react-tabulator/lib/styles.css"; // required styles
import "react-tabulator/css/tabulator_bootstrap5.min.css"; // theme
import { ReactTabulator } from "react-tabulator";

const Nights = () => {
  const [content, setContent] = useState([]);

  useEffect(() => {
    axios
      .get(`${process.env.REACT_APP_API_BASE}/display/nights/list`)
      .then((response) => {
        setContent(response.data);
      });
  }, []);

  const columns = [
    {
      title: "night",
      field: "night",
      width: 150,
      headerSort: false,
      formatter: "link",
      formatterParams: {
        labelField: "night",
        urlPrefix: `${process.env.PUBLIC_URL}/night/`,
      },
    },
    {
      title: "alerts",
      field: "alerts",
    },
    {
      title: "visits",
      field: "visits",
    },
    {
      title: "alerts/visit",
      field: "avg_alerts_per_visit",
      formatter: "money",
      formatterParams: { precision: 2, thousand: "_" },
      mutator: (value, data, type, params, component) => {
        if (data.visits && data.visits > 0) {
          return data.alerts / data.visits;
        } else {
          return 0;
        }
      }
    },
    {
      title: "time span",
      field: "day_span",
      formatter: function(cell, params) {
        var seconds = cell.getValue()*86400; // Convert days to seconds
        var repr = Math.floor(seconds % 60).toString() + "s";
        if (seconds <= 60) {
          return repr;
        }
        repr = ((Math.floor(seconds / 60) % 60).toString()) + "m " + repr;
        if (seconds <= 3600) {
          return repr;
        }
        repr = (Math.floor(seconds / 3600).toString()) + "h " + repr; 
        return repr;
      },
      hozAlign: "right",
    },
  ];

  // NB: fix height for performance reasons; without height constraint all content is rendered immediately
  return (
    <div>
      <div className="mx-1 mt-2">
      <h5 className="mb-3">Nights</h5>
      </div>
      <ReactTabulator columns={columns} data={content} height={400} />
    </div>
  );
};

export default Nights;
