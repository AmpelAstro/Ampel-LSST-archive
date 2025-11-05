import { initState } from "./CutoutPlots";
import React, { useState, useEffect } from "react";
import Plot from "react-plotly.js";
import axios from "axios"; // If using axios
import { useParams, useNavigate } from "react-router-dom";
import ReactJsonView from "@microlink/react-json-view";
import { JSONParse } from "json-with-bigint";

const LinkBadge = ({ diaObjectId, ssObjectId }) => {
  if (diaObjectId) {
    return (
      <a href={`/diaobject/${diaObjectId}`} className="btn btn-success mx-2">
        DIAObject
      </a>
    );
  }
  if (ssObjectId) {
    return (
      <a href={`/ssobject/${ssObjectId}`} className="btn btn-secondary mx-2">
        SSObject
      </a>
    );
  }
  return null;
};

const AlertView = () => {
  const { diaSourceId } = useParams();
  const navigate = useNavigate();

  // Keep a local id state, synced with the URL param
  const [idState, setIdState] = useState(diaSourceId);

  const [template, setTemplate] = useState(initState());
  const [science, setScience] = useState(initState());
  const [diff, setDiff] = useState(initState());
  const [alertData, setAlertData] = useState({});

  // Sync local state if the URL param changes (e.g., via navigate or manual URL entry)
  useEffect(() => {
    if (diaSourceId && diaSourceId !== idState) {
      setTemplate(initState());
      setScience(initState());
      setDiff(initState());
      setAlertData({});
      setIdState(diaSourceId);
    }
  }, [diaSourceId]);

  // Fetch alert details whenever the current id changes
  useEffect(() => {
    if (!idState) return;

    const fetchData = async () => {
      try {
        // parse response with JSONParse to handle BigInt
        const response = await axios.get(
          `http://localhost:8080/v1/display/alert/${idState}`,
          { transformResponse: [(data) => JSONParse(data)] }
        );
        setTemplate(response.data.cutouts.template);
        setScience(response.data.cutouts.science);
        setDiff(response.data.cutouts.difference);
        setAlertData(response.data.alert);
      } catch (error) {
        //   setError(error);
      } finally {
        //   setLoading(false);
      }
    };

    fetchData();
  }, [idState]);

  const handleButtonClick = () => {
    const fetchData = async () => {
      try {
        const response = await axios.get(
          `http://localhost:8080/v1/display/roulette`
        );
        const newId = response.data;
        setIdState(newId);
        // Update the URL param so the route reflects the new id
        navigate(`/alert/${newId}`);
      } catch (error) {
        //   setError(error);
      } finally {
        //   setLoading(false);
      }
    };

    fetchData();
  };

  return (
    <div className="mx-1 mt-2">
      <div className="d-flex justify-content-between align-items-center mb-3">
        <h5 className="mb-0">
          Alert {alertData.diaSourceId}
          <LinkBadge
            diaObjectId={alertData.diaSource?.diaObjectId}
            ssObjectId={alertData.diaSource?.ssObjectId}
          />
        </h5>
        <button
          type="button"
          className="btn btn-primary"
          onClick={handleButtonClick}
        >
          Hit me
        </button>
      </div>

      <div className="mb-3">
        <Plot data={template.data} layout={template.layout} />
        <Plot data={science.data} layout={science.layout} />
        <Plot data={diff.data} layout={diff.layout} />
      </div>
      <ReactJsonView
        src={alertData}
        name={"alert"}
        displayDataTypes={false}
        indentWidth={2}
        theme={"solarized"}
        bigNumber={BigInt}
      />
    </div>
  );
};

export default AlertView;
