import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";

const SSObjectView = () => {
  const { ssObjectId } = useParams();

  // Keep a local id state, synced with the URL param
  const [idState, setIdState] = useState(ssObjectId);

  // Sync local state if the URL param changes (e.g., via navigate or manual URL entry)
  useEffect(() => {
    if (ssObjectId && ssObjectId !== idState) {
      setIdState(ssObjectId);
    }
  }, [ssObjectId]);

  return (
    <div className="mx-1 mt-2">
      <div className="d-flex justify-content-between align-items-center mb-3">
        <h5 className="mb-0">
          SSObject {idState}
        </h5>
      </div>
    </div>
  );
};

export default SSObjectView;
