import { initState, Cutout } from "./CutoutPlots";
import { useState, useEffect } from "react";
import axios from "axios";
import { useParams } from "react-router-dom";

const TemplateCollection = ({ templates }) => {
  console.log(templates);

  return (
    <div className="d-flex flex-wrap gap-3">
      {Object.entries(templates).map(([band, payload]) => (
        <div key={band}>
          {console.log(payload)}
          <Cutout
            payload={{
              data: payload.data,
              layout: { ...initState().layout, ...payload.layout },
            }}
          />
        </div>
      ))}
    </div>
  );
};

const TemplateView = () => {
  const { diaObjectId } = useParams();

  // Keep a local id state, synced with the URL param
  const [idState, setIdState] = useState(diaObjectId);

  const [templates, setTemplates] = useState({});

  // Sync local state if the URL param changes (e.g., via navigate or manual URL entry)
  useEffect(() => {
    if (diaObjectId && diaObjectId !== idState) {
      setIdState(diaObjectId);
    }
  }, [diaObjectId, idState]);

  // Fetch alert details whenever the current id changes
  useEffect(() => {
    if (!idState) return;

    const fetchData = async () => {
      try {
        // parse response with JSONParse to handle BigInt
        const response = await axios.get(
          `${process.env.REACT_APP_API_BASE}/display/diaobject/${idState}/templates`
        );
        setTemplates(response.data);
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
        <h5 className="mb-0">DIAObject {idState}</h5>
      </div>
      <TemplateCollection templates={templates} />
    </div>
  );
};

export default TemplateView;
