import { initState } from "./CutoutPlots";
import React, { useState, useEffect } from "react";
import Plot from "react-plotly.js";
import axios from "axios";
import { useParams, useNavigate } from "react-router-dom";
import ReactJsonView from "@microlink/react-json-view";
import { JSONParse } from "json-with-bigint";

const LinkBadge = ({ diaObjectId, ssObjectId }) => {
  if (diaObjectId) {
    return (
      <a
        href={`${process.env.PUBLIC_URL}/diaobject/${diaObjectId}`}
        className="btn btn-success mx-2"
      >
        DIAObject
      </a>
    );
  }
  if (ssObjectId) {
    return (
      <a
        href={`${process.env.PUBLIC_URL}/ssobject/${ssObjectId}`}
        className="btn btn-secondary mx-2"
      >
        SSObject
      </a>
    );
  }
  return null;
};

const Cutout = ({ payload }) => {
  return (
    <Plot
      data={payload.data}
      layout={payload.layout}
      config={{ displayModeBar: false }}
    />
  );
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
  }, [diaSourceId, idState]);

  // Fetch alert details whenever the current id changes
  useEffect(() => {
    if (!idState) return;

    const fetchData = async () => {
      try {
        // parse response with JSONParse to handle BigInt
        const response = await axios.get(
          `${process.env.REACT_APP_API_BASE}/display/alert/${idState}`,
          { transformResponse: [(data) => JSONParse(data)] }
        );
        setTemplate((prevState) => ({
          data: response.data.cutouts.template.data,
          layout: {
            ...prevState.layout,
            ...response.data.cutouts.template.layout,
          },
        }));
        setScience((prevState) => ({
          data: response.data.cutouts.science.data,
          layout: {
            ...prevState.layout,
            ...response.data.cutouts.science.layout,
          },
        }));
        setDiff((prevState) => ({
          data: response.data.cutouts.difference.data,
          layout: {
            ...prevState.layout,
            ...response.data.cutouts.difference.layout,
          },
        }));
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
          `${process.env.REACT_APP_API_BASE}/display/roulette`
        );
        const newId = response.data;
        // Update the URL param so the route reflects the new id
        navigate(`${process.env.PUBLIC_URL}/alert/${newId}`);
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
        <Cutout payload={template} />
        <Cutout payload={science} />
        <Cutout payload={diff} />
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
