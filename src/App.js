import AlertView from "./Components/AlertView";
import { BrowserRouter, Routes, Route } from "react-router-dom";

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/alert/:diaSourceId" element={<AlertView />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
