import { NavLink, Route, Routes } from "react-router-dom";
import TasksPage from "./pages/TasksPage.jsx";
import TaskDetailPage from "./pages/TaskDetailPage.jsx";
import WorkersPage from "./pages/WorkersPage.jsx";
import MetricsPage from "./pages/MetricsPage.jsx";
import DlqPage from "./pages/DlqPage.jsx";

export default function App() {
  return (
    <div className="app">
      <aside className="sidebar">
        <h1 className="brand">Conductor</h1>
        <p className="tagline">Task Queue Dashboard</p>
        <nav className="nav">
          <NavLink to="/" end>
            Tasks
          </NavLink>
          <NavLink to="/workers">Workers</NavLink>
          <NavLink to="/metrics">Metrics</NavLink>
          <NavLink to="/dlq">Dead Letter</NavLink>
        </nav>
      </aside>
      <main className="content">
        <Routes>
          <Route path="/" element={<TasksPage />} />
          <Route path="/tasks/:id" element={<TaskDetailPage />} />
          <Route path="/workers" element={<WorkersPage />} />
          <Route path="/metrics" element={<MetricsPage />} />
          <Route path="/dlq" element={<DlqPage />} />
        </Routes>
      </main>
    </div>
  );
}
