import AlertView from "./Components/AlertView";
import DIAObjectView from "./Components/DIAObjectView";
import SSObjectView from "./Components/SSObjectView";
import { BrowserRouter, Routes, Route } from "react-router-dom";

function App() {
  return (
    <BrowserRouter basename={process.env.PUBLIC_URL}>
      <Routes>
        <Route path="/alert/:diaSourceId" element={<AlertView />} />
        <Route path="/diaobject/:diaObjectId" element={<DIAObjectView />} />
        <Route path="/ssobject/:ssObjectId" element={<SSObjectView />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
