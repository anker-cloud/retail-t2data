// import React from 'react';
// import { NavLink } from 'react-router-dom';
// import './VerticalNav.css';

// // Define SVG icons
// const ChatIcon = () => (
//   <svg className="nav-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
//     <path d="M20 2H4c-1.1 0-1.99.9-1.99 2L2 22l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm-2 12H6v-2h12v2zm0-3H6V9h12v2zm0-3H6V6h12v2z"/>
//   </svg>
// );

// const DataIcon = () => (
//   <svg className="nav-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
//     <path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 16H5V5h14v14zM7 10h2v7H7zm4-3h2v10h-2zm4 6h2v4h-2z"/>
//   </svg>
// );

// const ArchitectureIcon = () => (
//   <svg className="nav-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
//     <path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/>
//   </svg>
// );

// const VerticalNav = () => {
//   return (
//     <nav className="vertical-nav">
//       <div className="logo-container">
//         <h5>Powered by Mahindra Innovation</h5>
//       </div>
//       <ul>
//         <li>
//           <NavLink to="/" className={({ isActive }) => isActive ? "active-link" : ""}>
//             <ChatIcon />
//             Chatbot
//           </NavLink>
//         </li>
//         <li>
//           <NavLink to="/data-preview" className={({ isActive }) => isActive ? "active-link" : ""}>
//             <DataIcon />
//             Dataset Preview
//           </NavLink>
//         </li>
//         <li>
//           <NavLink to="/solution-architecture" className={({ isActive }) => isActive ? "active-link" : ""}>
//             <ArchitectureIcon />
//             Solution Design
//           </NavLink>
//         </li>
//       </ul>
//     </nav>
//   );
// };

// export default VerticalNav;
import React from 'react';
import { NavLink } from 'react-router-dom';
import './VerticalNav.css';

// Import logo properly from assets folder (no dashes in variable names)
import projectvihaanlogo from '../../assets/project-vihaan-logo.png'; // make sure the file & path exist

// Define SVG icons
const ChatIcon = () => (
  <svg className="nav-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
    <path d="M20 2H4c-1.1 0-1.99.9-1.99 2L2 22l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm-2 12H6v-2h12v2zm0-3H6V9h12v2zm0-3H6V6h12v2z"/>
  </svg>
);

const DataIcon = () => (
  <svg className="nav-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
    <path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 16H5V5h14v14zM7 10h2v7H7zm4-3h2v10h-2zm4 6h2v4h-2z"/>
  </svg>
);

const ArchitectureIcon = () => (
  <svg className="nav-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
    <path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/>
  </svg>
);

const VerticalNav = () => {
  return (
    <nav className="vertical-nav">
      <div className="logo-container">
        <h5>Powered by MDP</h5>
      </div>

      <ul>
        <li>
          <NavLink to="/" className={({ isActive }) => isActive ? "active-link" : ""}>
            {/* <ChatIcon /> */}
            Chatbot
          </NavLink>
        </li>
        <li>
          <NavLink to="/data-preview" className={({ isActive }) => isActive ? "active-link" : ""}>
            {/* <DataIcon /> */}
            Dataset Preview
          </NavLink>
        </li>
        <li>
          <NavLink to="/solution-architecture" className={({ isActive }) => isActive ? "active-link" : ""}>
            {/* <ArchitectureIcon /> */}
            Solution Design
          </NavLink>
        </li>
      </ul>

      {/* Logo at bottom */}
      <div className="bottom-logo">
        <img src={projectvihaanlogo} alt="Project Vihaan Logo" />
      </div>
    </nav>
  );
};

export default VerticalNav;
