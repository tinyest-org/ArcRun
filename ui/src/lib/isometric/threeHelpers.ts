import * as THREE from 'three';

export const TASK_BOX_SIZE = 0.8;
export const TASK_SPACING = 1.1;
export const GROUP_PADDING = 0.5;

export const taskGeometry = new THREE.BoxGeometry(TASK_BOX_SIZE, TASK_BOX_SIZE, TASK_BOX_SIZE);
export const arrowGeometry = new THREE.ConeGeometry(0.12, 0.3, 6);

export function colorToHex(color: string): number {
  return parseInt(color.replace('#', ''), 16);
}

export function createTextSprite(text: string, fontSize: number = 48): THREE.Sprite {
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d')!;
  canvas.width = 512;
  canvas.height = 128;
  ctx.fillStyle = 'rgba(255, 255, 255, 0.8)';
  ctx.font = `${fontSize}px monospace`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText(text, 256, 64);

  const texture = new THREE.CanvasTexture(canvas);
  texture.needsUpdate = true;
  const material = new THREE.SpriteMaterial({ map: texture, transparent: true });
  const sprite = new THREE.Sprite(material);
  sprite.scale.set(4, 1, 1);
  sprite.userData.isLabel = true;
  return sprite;
}

export function disposeObject(obj: THREE.Object3D): void {
  if (obj instanceof THREE.Mesh) {
    if (obj.geometry !== taskGeometry && obj.geometry !== arrowGeometry) {
      obj.geometry.dispose();
    }
    if (Array.isArray(obj.material)) {
      obj.material.forEach((m) => m.dispose());
    } else {
      obj.material.dispose();
    }
  } else if (obj instanceof THREE.Line || obj instanceof THREE.LineSegments) {
    obj.geometry.dispose();
    if (Array.isArray(obj.material)) {
      obj.material.forEach((m) => m.dispose());
    } else {
      (obj.material as THREE.Material).dispose();
    }
  } else if (obj instanceof THREE.Sprite) {
    (obj.material as THREE.SpriteMaterial).map?.dispose();
    obj.material.dispose();
  }
}
