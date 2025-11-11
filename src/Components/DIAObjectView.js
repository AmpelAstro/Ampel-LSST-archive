import { initState } from "./CutoutPlots";
import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import Plot from "react-plotly.js";
import axios from "axios";

const DIAObjectView = () => {
  const { diaObjectId } = useParams();

  // Keep a local id state, synced with the URL param
  const [idState, setIdState] = useState(diaObjectId);

  const [lightcurve, setLightcurve] = useState(initState());
  const [centroid, setCentroid] = useState(initState());

  // Sync local state if the URL param changes (e.g., via navigate or manual URL entry)
  useEffect(() => {
    if (diaObjectId && diaObjectId !== idState) {
      setIdState(diaObjectId);
    }
  }, [diaObjectId]);

  // Fetch alert details whenever the current id changes
  useEffect(() => {
    if (!idState) return;

    const fetchData = async () => {
      try {
        // parse response with JSONParse to handle BigInt
        const response = await axios.get(
          `http://localhost:8080/v1/display/diaobject/${idState}/summaryplots`
        );
        setLightcurve(response.data.lightcurve);
        setCentroid(response.data.centroid);
      } catch (error) {
        //   setError(error);
      } finally {
        //   setLoading(false);
      }
    };

    fetchData();
  }, [idState]);

  return (
    <div className="mx-1 mt-2">
      <div className="d-flex justify-content-between align-items-center mb-3">
        <h5 className="mb-0">
          DIAObject {idState}
        </h5>
      </div>
      <div className="mb-3">
        <Plot layout={lightcurve.layout} data={lightcurve.data} />
        <Plot layout={centroid.layout} data={centroid.data} />
      </div>
    </div>
  );
};

export default DIAObjectView;
