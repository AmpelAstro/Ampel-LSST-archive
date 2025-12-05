import AlertView from "./Components/AlertView";
import DIAObjectView from "./Components/DIAObjectView";
import SSObjectView from "./Components/SSObjectView";
import AlertsByNight from "./Components/AlertsByNight";
import Nights from "./Components/Nights";
import TemplateView from "./Components/TemplateView";
import { BrowserRouter, Routes, Route } from "react-router-dom";

function App() {
  return (
    <BrowserRouter basename={process.env.PUBLIC_URL}>
      <Routes>
        <Route path="/alert/:diaSourceId" element={<AlertView />} />
        <Route path="/diaobject/:diaObjectId" element={<DIAObjectView />} />
        <Route path="/diaobject/:diaObjectId/templates" element={<TemplateView />} />
        <Route path="/ssobject/:ssObjectId" element={<SSObjectView />} />
        <Route path="/night/:nightId" element={<AlertsByNight />} />
        <Route path="/nights" element={<Nights />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
