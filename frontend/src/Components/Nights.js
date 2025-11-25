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
