import { initState } from "./CutoutPlots";
import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";

const DIAObjectView = () => {
  const { diaObjectId } = useParams();

  // Keep a local id state, synced with the URL param
  const [idState, setIdState] = useState(diaObjectId);

  // Sync local state if the URL param changes (e.g., via navigate or manual URL entry)
  useEffect(() => {
    if (diaObjectId && diaObjectId !== idState) {
      setIdState(diaObjectId);
    }
  }, [diaObjectId]);

  return (
    <div className="mx-1 mt-2">
      <div className="d-flex justify-content-between align-items-center mb-3">
        <h5 className="mb-0">
          DIAObject {idState}
        </h5>
      </div>
    </div>
  );
};

export default DIAObjectView;
