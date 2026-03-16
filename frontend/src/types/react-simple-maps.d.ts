declare module 'react-simple-maps' {
  import * as React from 'react';

  export interface GeographyItem {
    rsmKey: string;
    id: string;
    type: string;
    properties: { name: string; [key: string]: unknown };
    geometry: { type: string; coordinates: unknown };
  }

  export interface GeographyStyleState {
    fill?: string;
    stroke?: string;
    strokeWidth?: number;
    outline?: string;
    opacity?: number;
  }

  export interface ComposableMapProps {
    projection?: string;
    projectionConfig?: {
      scale?: number;
      center?: [number, number];
      rotate?: [number, number, number];
    };
    width?: number;
    height?: number;
    style?: React.CSSProperties;
    className?: string;
    children?: React.ReactNode;
  }

  export interface GeographiesProps {
    geography: string | object;
    children: (props: { geographies: GeographyItem[] }) => React.ReactNode;
  }

  export interface GeographyProps {
    geography: GeographyItem;
    fill?: string;
    stroke?: string;
    strokeWidth?: number;
    className?: string;
    style?: {
      default?: GeographyStyleState;
      hover?: GeographyStyleState;
      pressed?: GeographyStyleState;
    };
    onClick?: (geo: GeographyItem, event: React.MouseEvent) => void;
    onMouseEnter?: (geo: GeographyItem, event: React.MouseEvent) => void;
    onMouseLeave?: (geo: GeographyItem, event: React.MouseEvent) => void;
  }

  export const ComposableMap: React.FC<ComposableMapProps>;
  export const Geographies: React.FC<GeographiesProps>;
  export const Geography: React.FC<GeographyProps>;
}
